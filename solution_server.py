#!/usr/bin/env python3
"""
CS3103 Assignment 4 - LiveRecall naive working baseline.

This starter is intentionally useful but incomplete. It provides the required
CLI and a basic WebSocket broadcaster so that caption_source.py -> server ->
browser/load clients works on day one.

It DOES NOT implement the graded networking mechanisms:
  - revision-state management or deduplication
  - recent-history recovery or late-join recovery
  - bounded per-client application queues
  - slow-client isolation
  - semantic coalescing / backpressure
  - constrained-bandwidth overload handling

Required evaluator command:
    python solution_server.py --port 8765

Teams should replace or substantially extend the implementation while keeping
this command-line interface and the protocol in the assignment specification.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Any, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from data_queue import SourceQueue

# Max segments queued per client (one entry per segment).
MAX_PENDING_SEGMENTS = 256
# Client is lagging once a ping waits this long beyond its base RTT.
LAG_THRESHOLD_S = 0.25
# Safety cap on frames sent since the last acknowledged ping.
MAX_UNACKED_FRAMES = 64
# Pacing after lag
PACE_DRAIN_FRACTION = 0.5
PACE_DRAIN_FRAMES = 16.0
PACE_HOLD_FRACTION = 0.8
PACE_HOLD_S = 60.0
MAX_PACE_HOLD_S = 240.0
# Lag within this long after a release doubles the next hold.
PACE_RETEST_S = 30.0
MIN_PACE_RATE = 0.2  # frames/s
# Max finals packed into one catch-up history frame.
MAX_BATCH_ITEMS = 64
# Max segments remembered in last_sent.
SENT_STATE_SEGMENTS = 512

# Experiment toggles via env vars, e.g. LIVERECALL_FLOW_CONTROL=0 python run_demo.py ...
FLOW_CONTROL = os.environ.get("LIVERECALL_FLOW_CONTROL", "1") != "0"
BATCH_FINALS = os.environ.get("LIVERECALL_BATCH_FINALS", "1") != "0"

# Connected student clients.
CLIENTS: set[ClientChannel] = set()
SOURCEQUEUE: SourceQueue = SourceQueue(90)


def parse_caption(msg: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Return a caption with only the protocol fields, or None if malformed."""
    try:
        segment = int(msg["segment"])
        revision = int(msg["revision"])
        source_ts = float(msg["source_ts"])
    except (KeyError, TypeError, ValueError, OverflowError):
        return None
    if segment < 1 or revision < 1:
        return None
    return {
        "type": "CAPTION_UPDATE",
        "session": msg.get("session"),
        "segment": segment,
        "revision": revision,
        "source_ts": source_ts,
        "final": bool(msg.get("final")),
        "text": str(msg.get("text", "")),
    }


def int_field(msg: dict[str, Any], name: str, default: int) -> int:
    """Return a non-negative integer field, or default if it is unusable."""
    try:
        return max(0, int(msg.get(name, default)))
    except (TypeError, ValueError, OverflowError):
        return default


class ClientChannel:
    """Bounded coalescing outbound queue plus sender task for one client."""

    def __init__(self, ws, client_id, history: list[dict[str, Any]]) -> None:
        self.ws = ws
        self.client_id = client_id
        self.history_frame = json.dumps({"type": "HISTORY", "items": history})
        # segment -> (revision, final, caption, frame), oldest segment first.
        self.pending: dict[int, tuple[int, bool, dict[str, Any], str]] = {}
        # segment -> highest revision already sent.
        self.last_sent: dict[int, int] = {}
        for item in history:
            self._mark_sent(int(item["segment"]), int(item["revision"]))
        self.wakeup = asyncio.Event()
        self.overflowed = False

        # Lag detection (one outstanding ping at a time).
        self.frames_sent = 0
        self.frames_acked = 0
        self.ping_sent_at: Optional[float] = None
        self.base_rtt: Optional[float] = None
        self.was_blocked = False
        # (time, frames_acked) at connect or at the previous late pong.
        self.rate_mark = (asyncio.get_running_loop().time(), 0)

        # Pacing (pace_rate 0 = unpaced).
        self.pace_rate = 0.0
        self.pace_set_at = 0.0
        self.pace_hold_s = PACE_HOLD_S
        self.released_at: Optional[float] = None
        self.last_write_at = 0.0

        # Stats counters.
        self.coalesced = 0
        self.batches = 0
        self.lag_events = 0

    def enqueue(self, caption: dict[str, Any], frame: str) -> None:
        segment = caption["segment"]
        revision = caption["revision"]
        if revision <= self.last_sent.get(segment, 0):
            return  # already sent this revision or a newer one
        queued = self.pending.get(segment)
        if queued is not None:
            queued_revision, queued_final, _, _ = queued
            if queued_final or revision <= queued_revision:
                return  # never replace a final or a newer revision
            self.coalesced += 1  # newer revision replaces the unsent older one
        elif len(self.pending) >= MAX_PENDING_SEGMENTS:
            self._shed()
            if self.overflowed:
                return
        self.pending[segment] = (revision, caption["final"], caption, frame)
        self.wakeup.set()

    def reset(self) -> None:
        self.pending.clear()
        self.last_sent.clear()

    def _shed(self) -> None:
        """Queue full: drop provisionals; if still full of finals, close the client."""
        for segment in [s for s, entry in self.pending.items() if not entry[1]]:
            del self.pending[segment]
            self.coalesced += 1
        if len(self.pending) >= MAX_PENDING_SEGMENTS:
            self.overflowed = True
            self.wakeup.set()

    async def run(self) -> None:
        try:
            await self._write(self.history_frame)
            while True:
                if self.overflowed:
                    await self.ws.close(code=1013, reason="too far behind; reconnect to resume")
                    return
                if FLOW_CONTROL and self.ping_sent_at is None and self.frames_sent > self.frames_acked:
                    await self._probe()
                if self.pending and not self._blocked():
                    delay = self._pace_delay()
                    if delay > 0:
                        await asyncio.sleep(delay)  # updates coalesce meanwhile
                        continue
                    await self._send_next()
                    continue
                self.wakeup.clear()
                await self.wakeup.wait()
        except ConnectionClosed:
            pass

    def _blocked(self) -> bool:
        """True while the client has not read what we already sent."""
        if not FLOW_CONTROL or self.ping_sent_at is None:
            return False
        waited = asyncio.get_running_loop().time() - self.ping_sent_at
        if (waited - (self.base_rtt or 0.0) > LAG_THRESHOLD_S
                or self.frames_sent - self.frames_acked >= MAX_UNACKED_FRAMES):
            if not self.was_blocked:
                self.lag_events += 1
            self.was_blocked = True
            return True
        return False

    def _rate_limit(self, now: float) -> float:
        """Current frames/s allowed for this client (0 = unpaced)."""
        if self.pace_rate <= 0:
            return 0.0
        t = now - self.pace_set_at
        if t < PACE_DRAIN_FRAMES / self.pace_rate:
            limit = self.pace_rate * PACE_DRAIN_FRACTION
        elif t < self.pace_hold_s:
            limit = self.pace_rate * PACE_HOLD_FRACTION
        else:
            # Release; re-measure the rate from here if it lags again.
            self.pace_rate = 0.0
            self.released_at = now
            self.rate_mark = (now, self.frames_acked)
            return 0.0
        return max(limit, MIN_PACE_RATE)

    def _pace_delay(self) -> float:
        now = asyncio.get_running_loop().time()
        limit = self._rate_limit(now)
        if limit <= 0:
            return 0.0
        return self.last_write_at + 1.0 / limit - now

    async def _send_next(self) -> None:
        # While lagging or paced, send all backlogged finals in one HISTORY frame.
        degraded = self.was_blocked or self.pace_rate > 0
        finals = [entry for entry in self.pending.values() if entry[1]]
        if len(finals) < 2:
            self.was_blocked = False  # backlog flushed
        if BATCH_FINALS and degraded and len(finals) >= 2:
            batch = finals[:MAX_BATCH_ITEMS]
            for revision, _, caption, _ in batch:
                del self.pending[caption["segment"]]
                self._mark_sent(caption["segment"], revision)
            self.batches += 1
            await self._write(json.dumps(
                {"type": "HISTORY", "items": [entry[2] for entry in batch]}))
            return

        segment = next(iter(self.pending))
        revision, _, _, frame = self.pending.pop(segment)
        self._mark_sent(segment, revision)
        await self._write(frame)

    async def _write(self, frame: str) -> None:
        await self.ws.send(frame)
        self.frames_sent += 1
        self.last_write_at = asyncio.get_running_loop().time()

    async def _probe(self) -> None:
        """Ping; the pong confirms the client has read every earlier frame."""
        mark = self.frames_sent
        self.ping_sent_at = asyncio.get_running_loop().time()
        pong_waiter = await self.ws.ping()
        pong_waiter.add_done_callback(lambda fut: self._on_pong(fut, mark))

    def _on_pong(self, fut: asyncio.Future, mark: int) -> None:
        if fut.cancelled() or fut.exception() is not None:
            return
        now = asyncio.get_running_loop().time()
        rtt = now - self.ping_sent_at if self.ping_sent_at is not None else 0.0
        late = rtt - (self.base_rtt or 0.0) > LAG_THRESHOLD_S
        if not late:
            self.base_rtt = rtt if self.base_rtt is None else min(self.base_rtt, rtt)
        self.frames_acked = max(self.frames_acked, mark)
        self.ping_sent_at = None
        if late:
            # Read rate = frames acked since the last late pong / time elapsed.
            mark_time, mark_acked = self.rate_mark
            if now - mark_time > 1.0 and self.frames_acked > mark_acked:
                if self.released_at is not None and now - self.released_at < PACE_RETEST_S:
                    self.pace_hold_s = min(self.pace_hold_s * 2, MAX_PACE_HOLD_S)
                else:
                    self.pace_hold_s = PACE_HOLD_S
                self.pace_rate = (self.frames_acked - mark_acked) / (now - mark_time)
                self.pace_set_at = now
            self.rate_mark = (now, self.frames_acked)
        self.wakeup.set()

    def _mark_sent(self, segment: int, revision: int) -> None:
        if revision > self.last_sent.get(segment, 0):
            self.last_sent[segment] = revision
        if len(self.last_sent) > SENT_STATE_SEGMENTS:
            oldest = sorted(self.last_sent)[: SENT_STATE_SEGMENTS // 2]
            for old in oldest:
                del self.last_sent[old]


def fan_out(caption: dict[str, Any]) -> None:
    """Hand one accepted update to every client without awaiting any of them."""
    frame = json.dumps(caption, separators=(",", ":"))
    for channel in CLIENTS:
        channel.enqueue(caption, frame)


def reset_lecture() -> None:
    """Forget the old lecture, since a new one reuses its segment numbers."""
    SOURCEQUEUE.reset()
    for channel in CLIENTS:
        channel.reset()


async def handle_source(ws, hello: dict[str, Any]) -> None:
    session = hello.get("session")
    print(f"source connected: session={session}", flush=True)
    first_caption = True

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(msg, dict):
                continue

            msg_type = msg.get("type")
            if msg_type == "CAPTION_UPDATE":
                caption = parse_caption(msg)
                if caption is None:
                    continue
                # A new source connection starting at segment 1 is a new lecture.
                if first_caption and caption["segment"] == 1 and SOURCEQUEUE.history:
                    reset_lecture()
                first_caption = False
                if SOURCEQUEUE.add(caption):
                    fan_out(caption)
            elif msg_type == "SOURCE_DONE":
                # Test-control message. The specification does not require it
                # to be forwarded to clients.
                print("source finished", flush=True)
            else:
                # Forward compatibility: ignore fields/messages this baseline
                # doesn't understand rather than crashing.
                continue
    finally:
        print("source disconnected", flush=True)


async def handle_client(ws, hello: dict[str, Any]) -> None:
    client_id = hello.get("client_id")
    last_segment = int_field(hello, "last_segment", 0)
    last_revision = int_field(hello, "last_revision", 0)
    history_seconds = min(int_field(hello, "history_seconds", 60), 60)

    # No await between history snapshot and registration, so no gap or duplicate.
    history = SOURCEQUEUE.get_latest(window=history_seconds, segment=last_segment, revision=last_revision)
    channel = ClientChannel(ws, client_id, history)
    CLIENTS.add(channel)
    sender = asyncio.create_task(channel.run())
    print(f"client connected: id={client_id}; clients={len(CLIENTS)}", flush=True)
    try:
        # Student clients normally don't send further messages after hello.
        # Keep the WebSocket open until the client disconnects.
        async for _ in ws:
            pass
    finally:
        CLIENTS.discard(channel)
        sender.cancel()
        print(f"client disconnected: id={client_id}; clients={len(CLIENTS)}", flush=True)


async def handler(ws) -> None:
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        hello = json.loads(raw)
    except (asyncio.TimeoutError, ValueError):
        await ws.close(code=1008, reason="Expected a valid hello message")
        return

    msg_type = hello.get("type") if isinstance(hello, dict) else None
    if msg_type == "SOURCE_HELLO":
        await handle_source(ws, hello)
    elif msg_type == "CLIENT_HELLO":
        await handle_client(ws, hello)
    else:
        await ws.close(code=1008, reason="Expected SOURCE_HELLO or CLIENT_HELLO")


async def report_stats(interval: float) -> None:
    """Print queue/backpressure counters every interval seconds."""
    while True:
        await asyncio.sleep(interval)
        if not CLIENTS:
            continue
        channels = list(CLIENTS)
        print(
            f"stats: clients={len(channels)} "
            f"pending_total={sum(len(c.pending) for c in channels)} "
            f"pending_max={max(len(c.pending) for c in channels)} "
            f"lagging_now={sum(c.was_blocked for c in channels)} "
            f"paced_now={sum(c.pace_rate > 0 for c in channels)} "
            f"lag_events={sum(c.lag_events for c in channels)} "
            f"coalesced={sum(c.coalesced for c in channels)} "
            f"batches={sum(c.batches for c in channels)}",
            flush=True,
        )


async def main_async(port: int, stats_interval: float) -> None:
    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        max_size=2**20,
        ping_interval=20,
        ping_timeout=20,
    ):
        print(f"LiveRecall server listening on ws://0.0.0.0:{port}", flush=True)
        if stats_interval > 0:
            asyncio.create_task(report_stats(stats_interval))
        await asyncio.Future()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--stats-interval", type=float, default=10.0,
                        help="Seconds between queue stats lines (0 disables)")
    args = parser.parse_args()
    try:
        asyncio.run(main_async(args.port, args.stats_interval))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
