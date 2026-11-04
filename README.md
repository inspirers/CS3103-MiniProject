# Team 27 - LiveRecall

## Team members

- Boisen Arvid Peter / A0360356E / client: replace revisions rather than appending duplicates, automatic reconnect + correct resume state, usable final UI
- Lee Jin Timothy / A0309324J / bounded per-client application queues, slow-client isolation, semantic coalescing / backpressure
- Sanfo Bimal Thomas / A0309330N / Frontend client: bounded local history, 15/30/60-second recall views, HISTORY merge and deduplication, reconnect/recovery integration
- Foo Fang Han / A0301344W / handling source, source queue design, recent-history/late-join recovery, message deduplication

## Ubuntu setup

<!-- Exact commands from a fresh Ubuntu environment. -->

```console
# sudo apt update
# sudo apt install -y python3 python3-venv python3-pip iproute2 python-is-python3 python3-websockets
```

## Install

```console
$ python -m venv venv
$ source venv/bin/activate
$ python -m pip install -r requirements.txt
```

## Required server

```console
$ python solution_server.py --port 8765
```
Where 8765 is any valid port number

## Browser client

Exact commands/steps.
```console
$ python -m http.server 8000 --directory client
```
Where 8000 is any valid port number

## Public deterministic tests

<!-- Exact commands and configuration used for T1-T5, including independent runs. -->

### Test 1
```console
$ python run_demo.py --clients 20 --duration 20 --runs 3 --output-stem t1
```

### Test 2
```console
$ python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t2
```

### Test 3
```console
$ python run_demo.py --clients 100 --duration 60 --runs 3 \
  --slow-percent 10 --slow-ms 500 --output-stem t3
```

Tests 1 to 3 were executed on a Hyper-V virtual machine running Ubuntu 26.04.1 LTS (Linux kernel 7.0.0-38-generic), provisioned with 8 vCPUs (@ 3.69 GHz) and 16 GB of RAM. The underlying host machine is equipped with an Intel Core Ultra 9 285H CPU, 64 GB of RAM, and an NVIDIA RTX 5090 Laptop GPU.

---

## Mandatory deterministic application demo

<!-- Exact commands/steps for `caption_source.py`, browser clients, revision replacement, recall, late join, and reconnect/recovery. -->

Before starting, ensure no leftover traffic-control (`tc`) network impairment rules are active on the loopback interface (`lo`):
```console
# ./network_profiles/reset.sh lo
```
Open **three terminals** in the project root directory and run the following commands:

### Terminal 1 — Start the WebSocket Server
```console
$ python solution_server.py --port 8765
```

### Terminal 2 — Serve the Browser Client Files
```console
$ python -m http.server 8000 --directory client
```

Next, open **`http://127.0.0.1:8000`** (or any IPv4 loopback address in the `127.0.0.0/8` block, such as `http://127.254.254.254:8000`) in **at least two browser tabs or windows** (Tab 1 and Tab 2). Both tabs will display `Connection: connected (fresh client)` and store a unique `client_id` in `sessionStorage`.


### Terminal 3 — Start the Deterministic Caption Source
Run the caption source in 120-second demo mode so the full 60-second recall window can be populated and tested:

```console
$ python caption_source.py --duration 120
```

---

## 2. Step-by-Step Demonstration Walkthrough

### Step 1: Provisional vs. Final Captions & In-Place Revision Replacement
1. Watch the **Now** box and the **Recent captions** list in Tab 1 and Tab 2 as captions stream in.
2. **One row per segment:** Each segment occupies a single row in the transcript rather than appending a new line per revision.
3. **Provisional styling & replacement:** Provisional captions appear in *italic gray text* with a gray left border and a `REV <n>` tag. When a newer revision of the same segment arrives, it edits the existing row in place, briefly flashes yellow, and increments `replaced revisions` in the **Metrics** panel.
4. **Final immutability:** When `final: true` arrives for a segment, the row switches to **solid dark text** with a **green left border** and a `FINAL` tag. Once finalized, the segment is never overwritten by late or duplicate frames.

### Step 2: Local Recall (`What did I miss?` - 15 s / 30 s / 60 s)
1. Allow the caption source to run for at least 60 seconds so the client accumulates a full rolling local buffer (`LOCAL_HISTORY_SECONDS = 65 s`).
2. Verify that **What did I miss?** defaults to the **30 s** view (`What did I miss? (30 s)` is highlighted) and lists all finalized captions from the last 30 seconds along with their relative age (e.g., `-14 s`).
3. Click the **15 s**, **30 s**, and **60 s** buttons.
4. Confirm that switching between windows updates the recall view immediately from the tab's local `finals` state without sending any requests to the server (`from HISTORY` in the **Metrics** panel does not change when toggling recall windows).

### Step 3: Temporary Disconnection, Automatic Reconnect, and Recovery
1. In **Tab 1**, trigger a disconnection using either of the following:
   - Click the **Reconnect** button in the UI (closes the socket cleanly and triggers reconnect), or
   - Temporarily stop and restart `solution_server.py` in Terminal 1 to observe exponential backoff with jitter (`reconnecting in <X> s (attempt #<N>)…`).
2. While disconnected, the status badge turns yellow/red and the live transcript dims.
3. When Tab 1 reconnects, it automatically sends a `CLIENT_HELLO` containing its `last_segment`, `last_revision`, and `history_seconds: 60`.
4. Verify in Tab 1:
   - The green recovery banner appears showing the recovery outcome (e.g., `Recovered <N> finalized caption(s) from server history.`).
   - Any missed segments are inserted in segment order with **no duplicate final captions**.
   - The completeness check below the transcript confirms: `Nothing missing: all <N> finalized captions in the held window are here, none twice.`

### Step 4: Late-Joining Client Context Recovery
1. While `caption_source.py` is actively streaming (around 60–90 seconds into the run), open a **third browser tab** at `http://127.0.0.1:8000` (or click **New client id (late join)** in Tab 2, which clears `sessionStorage` and reloads as a fresh client).
2. The new client connects with `last_segment: 0`, `last_revision: 0`, and `history_seconds: 60`.
3. Verify that the late client immediately receives a `HISTORY` frame containing up to 60 seconds of prior finalized/latest captions, populates the **Recent captions** and **What did I miss?** views, and seamlessly continues receiving live `CAPTION_UPDATE` messages.

---

## 3. Testing with Network Impairment Profiles (`tc netem` / `tbf`)

To test how the server and client handle rate limits, delay/jitter, packet loss, or packet reordering on `lo` during the demo:

| Profile | Command | Effect on WebSocket / TCP Stream |
|---|---|---|
| **2 Mbit/s Rate Limit** | `# ./network_profiles/bandwidth_2mbit.sh` | Caps throughput at 2 Mbit/s (`32 kbit` burst) with a 400 ms queue latency limit. |
| **Delay + Jitter** | `# ./network_profiles/delay_100ms.sh` | Adds ~100 ms delay with ±20 ms normal jitter on `lo`. |
| **Delay + Jitter + 3% Loss** | `# ./network_profiles/delay_loss.sh` | Adds 120 ms (±30 ms) delay with 3% IP packet loss; triggers TCP retransmissions and head-of-line delay. |
| **3% Packet Loss Only** | `# ./network_profiles/loss_3pct.sh` | Simulates 3% IP packet loss; TCP guarantees ordered delivery at the cost of retransmission delay. |
| **Packet Reordering** | `# ./network_profiles/reorder.sh` | Applies 20 ms delay with 20% (50% correlation) IP packet reordering; TCP restores byte-stream order before reaching WebSocket. |
| **Reset / Clear Rules** | `# ./network_profiles/reset.sh` | Removes all traffic-control (`tc`) impairments on `lo`. |

---

## Design notes

<!-- Briefly describe: -->

### Revision and deduplication state
The server maintains a dictionary containing the most recent revisions of each segment, as well as whether each segment is final or not. Whenever the server receives a new `CAPTION_UPDATE` from the source, it drops the update if:
* The same or a newer revision of that segment has already been stored.
* The segment has already been marked as final.

On the client side, the browser maintains three related data structures: a `segments` map containing the latest accepted revision of each segment, a rolling `finals` list for recall, and a `rows` map connecting segments to their rendered DOM elements. All live and recovered captions pass through the same `applyCaption()` function, preventing `HISTORY` recovery from bypassing the normal revision and deduplication rules. The client keeps one row per segment on screen, so a newer revision simply replaces the older text in place. Once a segment is marked as final, it can never be overwritten, and any older (stale) or repeated (duplicate) revisions that arrive late are ignored.

Although the client cleans up visible finalized captions after 65 seconds to save memory, it remembers which segments were already finalized in a `finaled` deduplication map for 180 seconds. Since this is longer than the server's history window (90 s stored, at most 60 s served), an old final caption that was already cleaned up from the screen can never be added again after a reconnect. Furthermore, the resume high-water mark (`lastFinalSegment` and `lastFinalRevision`) is not reduced when old local history is pruned, ensuring that reconnecting clients do not incorrectly request already-processed older segments.


### Bounded server history

The server keeps the latest revision of each segment for the last 90 seconds (capped at 4096 segments). Whenever it receives a `CLIENT_HELLO`, it replies with a `HISTORY` frame containing only finalized captions newer than the client's `last_segment`/`last_revision`, within `history_seconds` (capped at 60), ordered oldest first.

On the browser side, to support the "What did I miss?" recall feature, the client retains the latest 65 seconds of finalized captions (selected over an exact 60-second window to provide a small margin for timestamp rounding and uneven caption intervals). After each accepted caption, the client calculates `cutoff = latest source timestamp − 65 seconds`. Finalized captions older than the cutoff are removed from the rolling list, segment map, and displayed transcript, whereas provisional captions are retained because they may still receive newer revisions.

The recall interface supports 15, 30, and 60-second windows (with 30 seconds selected by default) and is performed entirely from local client state without generating new WebSocket requests when switching views. The client filters only finalized captions whose `source_ts` falls inside the selected window and orders them from oldest to newest, while also reporting when the client has insufficient local history (such as shortly after a late join).

### Reconnect and late-join recovery
To prevent overloading the server during recovery, the client implements exponential backoff with jitter when scheduling reconnection attempts. Without randomized delays, a server outage or network interruption would cause all disconnected clients to retry simultaneously. In `scheduleReconnect()`, the base backoff scales exponentially (`RECONNECT_MIN_MS` * 2^exp, capped at an exponent of 10 and bounded by `RECONNECT_MAX_MS`) and is multiplied by a random jitter factor between 0.75 and 1.25 (± 25%). This spreads reconnection requests out over time, protecting the server from traffic spikes as it comes back online.

On every initial connection or reconnect, the client sends the most recent finalized segment and revision in `CLIENT_HELLO` and requests 60 seconds of recovery history. Requesting 60 seconds upfront ensures that all recall-window options (15, 30, and 60 seconds) can be populated locally after a late join or disconnection without needing another server request when the user switches views.

When a `HISTORY` message arrives, the client validates that the response contains an array, parses each caption using the same parser used for live updates, and sorts valid items from oldest to newest before passing each item through `applyCaption(caption, true)` to fill in any missing gaps without creating duplicates. Malformed, recovered, duplicate, and stale items are recorded in client-side metrics.

### Per-client buffering and backpressure policy

Each client has its own bounded queue and sender task, so a slow client only delays itself. While a client keeps up, it receives every revision in order. If its oldest queued message has waited over 1 s, the client is marked overloaded: we keep only the newest unsent revision of each segment, and a final is never replaced. TCP's buffers originally hid slow clients, so the server also pings each client every 0.3 s or after 16 unacknowledged frames. If a pong arrives more than 250 ms later than the client's usual round-trip time, or 64 frames are unacknowledged, we stop writing and let the backlog collect on the server, where old revisions are discarded. From the frames acknowledged between late pongs, we measure the client's real read rate and pace it at 50% of that rate to drain the backlog, then 80% for 10–60 s. While a client is overloaded or paced, its waiting finals are sent together in one `HISTORY` message of up to 64 items. If its queue still holds 2048 messages after dropping all old revisions and provisionals, we close the connection (code 1013) so it can reconnect and catch up through `HISTORY`.

### Overload behavior and trade-offs
To handle severe overload and degraded states, the server relies on two main optimizations:
1. **Flow control:** Monitors client responsiveness via ping probing to detect sudden bursts of lag when RTT exceeds the baseline threshold, and estimates how much data can reach the client from the number of frames acknowledged between late pongs and the time between them.
2. **Batch History:** Bundles multiple backlog updates into a single `HISTORY` frame when a client is severely overloaded and in a degraded state, significantly reducing framing overhead and maximizing data transfer when the sending rate is restricted.

Under harsh network conditions, there is a fundamental trade-off between message **freshness** and **completeness**:
* **Prioritizing Freshness (Coalescing):** Stricter flow control drops older provisional revisions in the queue to prevent stale messages from accumulating. While this drastically improves message freshness, flow control alone sends finals one per frame at the paced rate, so they fall behind and final completeness drops (59.55% in our harsh test). Combining flow control with `HISTORY` batching mitigates this by delivering queued finals together, restoring high final caption completeness (e.g., 95.21% under harsh impairment).
* **Prioritizing Completeness:** Preserving stale ("unfresh") messages in the client's queue guarantees that clients see a more complete set of intermediate revisions, but severely degrades caption age and latency metrics.
* **Disconnection and Unrecoverable Loss:** If a client's queue still holds `MAX_PENDING_MESSAGES` (2048) messages after dropping all old revisions and provisionals, the server closes the connection. If the client takes longer than 60 seconds to reconnect, finals older than 60 seconds at reconnect time are lost, because `HISTORY` serves at most 60 seconds.
* **Rate Estimation Delays:** Because the server uses RTT differences from ping packets to estimate client processing rates, rate information is inherently delayed and may adjust slowly to rapid network changes. A client's processing speed is only visible once its receive buffers fill and pongs are delayed.

<!-- ## Optional ASR bonus

Only if attempted. Keep these dependencies separate from the mandatory server path. -->
