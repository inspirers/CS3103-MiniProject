/*
 * Working but deliberately naive LiveRecall browser starter.
 *
 * Provided mechanical behavior:
 *   - opens a WebSocket
 *   - sends CLIENT_HELLO
 *   - visibly appends every received CAPTION_UPDATE
 *   - displays whether an update is provisional/final
 *
 * Graded TODOs:
 *   - replace revisions rather than appending duplicates (Implemented ?)
 *   - maintain >=60 s of finalized local state
 *   - implement 30 s default recall + 15/30/60 s views
 *   - automatic reconnect + correct resume state (Implemented)
 *   - HISTORY merge/deduplication
 *   - usable final UI (Implemented, small fixes remaining after the other stuff is fixed)
 */

const WS_URL = "ws://127.0.0.1:8765";
const SESSION = "L1";
const RECONNECT_MIN_MS = 300;
const RECONNECT_MAX_MS = 12000;
const MAX_EVENT_LINES = 40;
const DEFAULT_RECALL_SECONDS = 30;  
const LOCAL_HISTORY_SECONDS = 65;
const RECALL_WINDOWS = [15, 30, 60];

const statusEl = document.getElementById("status");
const liveEl = document.getElementById("live");
const recallEl = document.getElementById("recall");
const metaEl = document.getElementById("meta");
const recoveryEl = document.getElementById("recovery");
const activeEl = document.getElementById("active");
const missedEl = document.getElementById("missed");
const recallTitleEl = document.getElementById("recall-title");
const statsEl = document.getElementById("stats");
const eventsEl = document.getElementById("events");

const segments = new Map();  // segment -> caption
const rows = new Map();      // segment -> its DOM row
let finals = [];             // finalized captions, oldest first
let lastFinalSegment = 0;    // resume state reported to the server
let lastFinalRevision = 0;
let latestSourceTs = 0;      // newest source_ts seen = our idea of "now"
let lastCaptionAt = 0;       // browser clock when the last caption arrived
let recallSeconds = DEFAULT_RECALL_SECONDS;  // the recall window currently shown

const counts = {
  updates: 0,        // caption messages that were new information
  replaced: 0,       // newer revision that overwrote an older one
  duplicates: 0,     // same revision twice, ignored
  stale: 0,          // older revision after a newer/final one, ignored
  finals: 0,         // finalized captions stored
  recovered: 0,      // captions taken from HISTORY frames
  historyFrames: 0,  // HISTORY frames received
  ignored: 0,        // frames we could not understand
};

const conn = {
  ws: null,
  status: "disconnected",
  attempts: 0,
  reconnectTimer: null,
  reconnectInMs: 0,
  lastResumeFrom: 0,
  lastHistoryItems: 0,
  connectedAt: 0, 
};

function numericClientId() {
  const key = "liverecall-client-id";
  let value = sessionStorage.getItem(key);
  if (!value) {
    value = String(Math.floor(100000 + Math.random() * 900000));
    sessionStorage.setItem(key, value);
  }
  return Number(value);
}

// Arvid Peter Boisen 2026-10-30
// Keep only the protocol fields with the right types. Returning null for a
// malformed frame means one bad message can never break the UI. A missing
// source_ts on its own is tolerated, because dropping the item would lose a
// real caption; it only makes the age display approximate.
function readCaption(msg) {
  const segment = Number(msg?.segment);
  const revision = Number(msg?.revision);
  const sourceTs = Number(msg?.source_ts);
  if (!Number.isInteger(segment) || segment < 1) return null;
  if (!Number.isInteger(revision) || revision < 1) return null;
  return {
    segment,
    revision,
    source_ts: Number.isFinite(sourceTs) ? sourceTs : latestSourceTs, // Might be an issue here if sourceTs is invalid/missing on the first frame
    final: Boolean(msg.final),
    text: String(msg.text ?? ""),
  };
}

// Arvid Peter Boisen 2026-10-30
// A caption is new information only if we do not hold that segment yet, or it
// is a strictly newer revision of a segment that is not final. A final caption
// is durable state, so nothing is ever allowed to overwrite it, and an older
// revision that arrives late is dropped instead of going back on screen.
// Returns true when the caption was accepted.
function applyCaption(caption, fromHistory = false) {
  const held = segments.get(caption.segment);
  if (held) {
    if (held.final || caption.revision <= held.revision) {
      if (caption.revision === held.revision) counts.duplicates += 1;
      else counts.stale += 1;
      return false;
    }
    counts.replaced += 1;  // newer revision replaces the older one in place
  }

  counts.updates += 1;
  segments.set(caption.segment, caption);
  if (fromHistory) counts.recovered += 1;
  latestSourceTs = Math.max(latestSourceTs, caption.source_ts);
  lastCaptionAt = Date.now() / 1000;

  if (caption.final) {
    finals.push(caption);
    counts.finals += 1;
    // Resume state tracks only the newest finalized segment/revision.
    if (caption.segment > lastFinalSegment ||
        (caption.segment === lastFinalSegment && caption.revision > lastFinalRevision)) {
      lastFinalSegment = caption.segment;
      lastFinalRevision = caption.revision;
    }
  }

  renderCaption(caption);
  renderActive();
  pruneHistory();
  return true;
}

function pruneHistory() {
  // Keep a small margin beyond the required 60 seconds so captions on the
  // boundary are not lost because of timestamp rounding or uneven updates.
  const cutoff = latestSourceTs - LOCAL_HISTORY_SECONDS;
  const retainedFinals = [];

  for (const caption of finals) {
    if (caption.source_ts >= cutoff) {
      retainedFinals.push(caption);
      continue;
    }

    // Only remove the segment if this finalized caption is still its current
    // state. Provisional captions are deliberately left untouched.
    const held = segments.get(caption.segment);
    if (held?.final && held.revision === caption.revision) {
      segments.delete(caption.segment);

      const row = rows.get(caption.segment);
      if (row) row.remove();
      rows.delete(caption.segment);
    }
  }

  finals = retainedFinals;
}


// Arvid Peter Boisen 2026-10-30
// One row per segment. A newer revision edits the existing row (and briefly
// highlights it) rather than appending a duplicate line to the transcript.
function renderCaption(caption) {
  let row = rows.get(caption.segment);
  if (!row) {
    row = document.createElement("div");
    row.className = "caption";
    row.dataset.segment = String(caption.segment);
    const tag = document.createElement("span");
    tag.className = "tag";
    const text = document.createElement("span");
    text.className = "text";
    row.append(tag, text);
    insertRowInOrder(row, caption.segment);
    rows.set(caption.segment, row);
  }
  row.classList.toggle("final", caption.final);
  row.classList.toggle("provisional", !caption.final);
  row.querySelector(".tag").textContent = caption.final ? "final" : `rev ${caption.revision}`;
  row.querySelector(".text").textContent = caption.text;

  row.classList.remove("updated");
  void row.offsetWidth;
  row.classList.add("updated");

  const atBottom = liveEl.scrollHeight - liveEl.scrollTop - liveEl.clientHeight < 60;
  if (atBottom) liveEl.scrollTop = liveEl.scrollHeight;
}

// Arvid Peter Boisen 2026-10-30
// Keep the list ordered by segment number even if an old segment arrives late.
function insertRowInOrder(row, segment) {
  for (const existing of liveEl.children) {
    if (Number(existing.dataset.segment) > segment) {
      liveEl.insertBefore(row, existing);
      return;
    }
  }
  liveEl.appendChild(row);
}

// Arvid Peter Boisen 2026-10-30
// The sentence being spoken right now: the newest segment that is not final.
function renderActive() {
  let active = null;
  for (const caption of segments.values()) {
    if (!caption.final && (!active || caption.segment > active.segment)) active = caption;
  }
  activeEl.replaceChildren();
  if (!active) {
    const hint = document.createElement("span");
    hint.className = "hint";
    hint.textContent = finals.length ? "waiting for the next sentence\u2026" : "waiting for captions\u2026";
    activeEl.append(hint);
    return;
  }
  const tag = document.createElement("span");
  tag.className = "tag";
  tag.textContent = `segment ${active.segment} \u00b7 revision ${active.revision} \u00b7 provisional`;
  const text = document.createElement("span");
  text.className = "text";
  text.textContent = active.text;
  activeEl.append(tag, text);
}

// Arvid Peter Boisen 2026-10-30
// Render finalized captions already held by this tab for the selected window.
// This function never contacts the server.
function renderRecall(seconds) {
  const requestedSeconds = Number(seconds);
  recallSeconds = RECALL_WINDOWS.includes(requestedSeconds)
    ? requestedSeconds
    : DEFAULT_RECALL_SECONDS;

  for (const button of document.querySelectorAll("button[data-seconds]")) {
    button.setAttribute(
      "aria-pressed",
      String(Number(button.dataset.seconds) === recallSeconds),
    );
  }

  // "Now" is the newest caption we have seen, so the window is measured in
  // lecture time and is not affected by a clock difference with the server.
  const cutoff = latestSourceTs - recallSeconds;
  const shown = finals
    .filter((caption) => caption.final && caption.source_ts >= cutoff)
    .sort((left, right) =>
      left.source_ts - right.source_ts || left.segment - right.segment,
    );
  recallTitleEl.textContent =
    `Missing in the last ${recallSeconds} s: ${shown.length} finalized caption(s)`;

  recallEl.replaceChildren();
  if (!shown.length) {
    const note = document.createElement("div");
    note.className = "hint";
    note.textContent = finals.length
      ? `No finalized captions were received in this ${recallSeconds}-second window.`
      : "No finalized captions are stored locally yet.";
    recallEl.append(note);
  }

  for (const caption of shown) {
    const line = document.createElement("div");
    line.className = "recall-row";
    const age = document.createElement("span");
    age.className = "age";
    age.textContent = `-${Math.max(0, Math.round(latestSourceTs - caption.source_ts))} s`;
    const text = document.createElement("span");
    text.textContent = caption.text;
    line.append(age, text);
    recallEl.append(line);
  }

  // Be explicit when this tab has less local finalized context than requested,
  // for example shortly after joining or reconnecting.
  const oldestFinalTs = finals.length
    ? Math.min(...finals.map((caption) => caption.source_ts))
    : latestSourceTs;
  const covered = Math.max(0, latestSourceTs - oldestFinalTs);
  if (finals.length && covered < recallSeconds) {
    const note = document.createElement("div");
    note.className = "hint";
    note.textContent = `Only ${Math.round(covered)} s of finalized history is available locally; ` +
      `this tab may have joined or reconnected too recently to cover the full ${recallSeconds} s.`;
    recallEl.append(note);
  }
}

function helloFrame() {
  // Arvid Peter Boisen 2026-10-28
  // Send Hello frame,
  return {
    type: "CLIENT_HELLO",
    client_id: numericClientId(),
    session: SESSION,
    last_segment: lastFinalSegment,
    last_revision: lastFinalRevision,
    history_seconds: 30,
  };
}

function historySeconds() {
  // TODO Implement this
  return DEFAULT_RECALL_SECONDS;
}

function connect(reason = "initial") {
  // Arvid Peter Boisen 2026-10-28
  // If the client is already connected or trying to connect no need to try again
  if (conn.ws && (conn.ws.readyState === WebSocket.OPEN || conn.ws.readyState === WebSocket.CONNECTING)) {
    return;
  }
  clearTimeout(conn.reconnectTimer);
  conn.reconnectTimer = null;
  conn.status = "connecting";
  renderStatus();

  // Arvid Peter Boisen 2026-10-28
  // Try to connect to WebSocket
  try {
    conn.ws = new WebSocket(WS_URL);
  } catch (err) {
    logEvent(`cannot open socket: ${err}`);
    scheduleReconnect();
    return;
  }
  const socket = conn.ws;
  socket.addEventListener("open", () => {
    conn.status = "connected";
    conn.attempts = 0;
    conn.connectedAt = Date.now() / 1000;
    const hello = helloFrame();
    conn.lastResumeFrom = hello.last_segment;
    conn.lastResumeRevision = hello.last_revision;
    socket.send(JSON.stringify(hello));
    logEvent(`${reason}: connected, resuming after segment ${hello.last_segment} ` +
             `(${hello.history_seconds} s history requested)`);
    renderStatus();
  });

  socket.addEventListener("message", (event) => {
    // Arvid Peter Boisen 2026-10-28
    // parse incoming message as json then handle msg
    // on error increase ignored count
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch {
      counts.ignored += 1;
      renderMetrics();
      return;
    }
    handleFrame(msg);
  });

  socket.addEventListener("close", () => {
    conn.ws = null;
    conn.status = "reconnecting";
    logEvent("connection lost, reconnecting automatically");
    scheduleReconnect();
  });

  socket.addEventListener("error", () => {
    conn.status = "connection error";
    renderStatus();
  });
}

function scheduleReconnect() {
  conn.attempts += 1;

  // Arvid Peter Boisen 2026-10-28
  // Perform exponential backoff for retrying to connect
  // Limit exp to 10 to avoid overflow error, max reconnect time is also limited by RECONNECT_MAX_MS

  const exp = Math.min(conn.attempts - 1, 10);
  const backoff = Math.min(RECONNECT_MAX_MS, RECONNECT_MIN_MS * 2 ** exp);

  // Arvid Peter Boisen 2026-10-28
  // Introduce a small randomness so in case the server dies
  // and later comes online all clients does not try to reconnect at the exact same time

  const delay = Math.round(backoff * (0.75 + Math.random() * 0.5));
  conn.reconnectInMs = delay;
  conn.reconnectTimer = setTimeout(() => connect(`retry #${conn.attempts}`), delay);
  renderStatus();
}

function handleFrame(msg) {
  const type = msg?.type;
  if (type === "CAPTION_UPDATE") {
  const caption = readCaption(msg);
  if (!caption) counts.ignored += 1;
  else applyCaption(caption);
  } else if (type === "HISTORY") {
    mergeHistory(msg);
  } else {
    counts.ignored += 1; 
  }
  renderRecall(recallSeconds);
  renderMetrics();
}

function mergeHistory(msg) {
  counts.historyFrames += 1;

  if (!Array.isArray(msg?.items)) {
    counts.ignored += 1;
    conn.lastHistoryItems = 0;
    recoveryEl.hidden = false;
    recoveryEl.textContent = "Ignored a malformed HISTORY frame: items must be an array.";
    logEvent("ignored malformed HISTORY frame");
    return;
  }

  const validCaptions = [];
  let malformedItems = 0;

  // Validate every history item through the same parser used for live updates.
  for (const item of msg.items) {
    const caption = readCaption(item);
    if (!caption) {
      counts.ignored += 1;
      malformedItems += 1;
      continue;
    }
    validCaptions.push(caption);
  }

  // HISTORY should already be oldest first, but sorting a copy makes merging
  // deterministic even if a server sends valid items out of order.
  validCaptions.sort((left, right) =>
    left.source_ts - right.source_ts ||
    left.segment - right.segment ||
    left.revision - right.revision,
  );

  let recoveredItems = 0;
  for (const caption of validCaptions) {
    // applyCaption owns revision replacement, stale-update rejection, final
    // deduplication, local-history pruning, and the recovered counter.
    if (applyCaption(caption, true)) recoveredItems += 1;
  }

  const skippedItems = validCaptions.length - recoveredItems;
  conn.lastHistoryItems = recoveredItems;
  recoveryEl.hidden = false;

  if (recoveredItems > 0) {
    recoveryEl.textContent =
      `Recovered ${recoveredItems} finalized caption(s) from server history.`;
  } else if (validCaptions.length > 0) {
    recoveryEl.textContent =
      `History checked: all ${validCaptions.length} item(s) were already held or stale; ` +
      "no duplicate captions were added.";
  } else {
    recoveryEl.textContent = "History checked: no captions needed recovery.";
  }

  if (skippedItems > 0 && recoveredItems > 0) {
    recoveryEl.textContent += ` Skipped ${skippedItems} duplicate or stale item(s).`;
  }
  if (malformedItems > 0) {
    recoveryEl.textContent += ` Ignored ${malformedItems} malformed item(s).`;
  }

  logEvent(
    `HISTORY: ${recoveredItems} recovered, ${skippedItems} duplicate/stale, ` +
    `${malformedItems} malformed`,
  );
}

function renderStatus() {
  // Arvid Peter Boisen 2026-10-28
  // Display the connection status in the browser
  let text = conn.status;
  if (conn.status === "connecting" && conn.attempts > 0) {
    text = `connecting (attempt #${conn.attempts})\u2026`;
  } else if (conn.status === "reconnecting") {
    const secs = (conn.reconnectInMs / 1000).toFixed(1);
    text = `reconnecting in ${secs} s (attempt #${conn.attempts})\u2026`;
  } else if (conn.status === "connected") {
    text = conn.lastResumeFrom > 0
      ? `connected (resumed after segment #${conn.lastResumeFrom})`
      : "connected (fresh client)";
  }
  statusEl.textContent = text;
  statusEl.dataset.state = conn.status;
  document.body.dataset.state = conn.status;  

  const ago = lastCaptionAt ? Math.max(0, Math.round(Date.now() / 1000 - lastCaptionAt)) : 0;
  metaEl.textContent = `session ${SESSION} \u00b7 client #${numericClientId()} \u00b7 ` +
    `history held locally: ${finals.length} finalized captions \u00b7 last caption ${ago} s ago`;
}

// Metrics about captions
function renderMetrics() {
  // newest - oldest + 1 should equal how many captions are stored
  const oldest = finals.length ? finals[0].segment : 0;
  const missing = finals.length
    ? Math.max(0, lastFinalSegment - oldest + 1 - finals.length)
    : 0;
  missedEl.textContent = missing === 0
    ? `Nothing missing: all ${finals.length} finalized captions in the held window are here, none twice.`
    : `${missing} finalized caption(s) inside the held window never reached this client ` +
      "(the server history window may have expired).";

  const heldSeconds = finals.length ? Math.round(latestSourceTs - finals[0].source_ts) : 0;
  statsEl.textContent =
    `new captions  ${counts.updates}   replaced revisions ${counts.replaced}   from HISTORY ${counts.recovered}\n` +
    `ignored       ${counts.duplicates + counts.stale + counts.ignored}   duplicate revision ${counts.duplicates}   ` +
    `stale revision ${counts.stale}   malformed ${counts.ignored}\n` +
    `local state   ${finals.length} finals held, covering ${heldSeconds} s   ` +
    `resume state sent: segment ${conn.lastResumeFrom} revision ${conn.lastResumeRevision}`;
}


const events = [];
function logEvent(text) {
  const stamp = new Date().toISOString().slice(11, 19);
  events.push(`${stamp}  ${text}`);
  if (events.length > MAX_EVENT_LINES) events.shift();
  eventsEl.textContent = events.join("\n");
}

function bindControls() {
  for (const button of document.querySelectorAll("button[data-seconds]")) {
    button.addEventListener("click", () => renderRecall(Number(button.dataset.seconds)));
  }
  document.getElementById("reconnect").addEventListener("click", () => {
    // When doing reconnect close socket and then connect again
    if (conn.ws && conn.ws.readyState === WebSocket.OPEN) {
      conn.ws.close(1000, "student requested reconnect");
    } else {
      connect("reconnect now");
    }
  });
  document.getElementById("reset").addEventListener("click", () => {
    sessionStorage.removeItem("liverecall-client-id");
    location.reload();
  });
  document.addEventListener("visibilitychange", () => {
    // student switches tabs, minimizes or restores the browser window
    if (!document.hidden && (!conn.ws || conn.ws.readyState !== WebSocket.OPEN)) {
      connect("tab resumed");
    }
  });
  window.addEventListener("online", () => connect("browser back online"));
}

// Keep the reconnect countdown and the "last caption Ns ago" text moving.
setInterval(() => {
  renderStatus();
  renderMetrics();
}, 1000);

bindControls();
renderRecall(DEFAULT_RECALL_SECONDS);  // the recall panel opens on the default 30 s view
connect();
