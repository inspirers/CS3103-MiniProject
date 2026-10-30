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
 *   - replace revisions rather than appending duplicates
 *   - maintain >=60 s of finalized local state
 *   - implement 30 s default recall + 15/30/60 s views
 *   - automatic reconnect + correct resume state
 *   - HISTORY merge/deduplication
 *   - usable final UI
 */

const WS_URL = "ws://127.0.0.1:8765";
const statusEl = document.getElementById("status");
const liveEl = document.getElementById("live");
const recallEl = document.getElementById("recall");

const RECONNECT_MIN_MS = 300;
const RECONNECT_MAX_MS = 12000;

let ws;

function numericClientId() {
  const key = "liverecall-client-id";
  let value = sessionStorage.getItem(key);
  if (!value) {
    value = String(Math.floor(100000 + Math.random() * 900000));
    sessionStorage.setItem(key, value);
  }
  return Number(value);
}

function appendNaively(msg) {
  const row = document.createElement("div");
  row.className = "naive-line";
  const state = msg.final ? "FINAL" : "PROVISIONAL";
  row.textContent = `[${state}] segment ${msg.segment} rev ${msg.revision}: ${msg.text}`;
  liveEl.appendChild(row);
  liveEl.scrollTop = liveEl.scrollHeight;
}

const conn = {
  ws: null,
  status: "disconnected",
  attempts: 0,
  reconnectTimer: null,
  reconnectInMs: 0,
  lastResumeFrom: 0,
  lastHistoryItems: 0,
};

function helloFrame() {
  // Arvid Peter Boisen 2026-10-28
  // Send Hello frame,
  // TODO implement last segment and last revision, make history_seconds dynamic
  return {
    type: "CLIENT_HELLO",
    client_id: numericClientId(),
    session: "L1",
    last_segment: 0,
    last_revision: 0,
    history_seconds: 30,
  };
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
  conn.lastResumeFrom = 1; // TODO Update this value from storage
  render();

  // Arvid Peter Boisen 2026-10-28
  // Try to connect to WebSocket
  let ws;
  try {
    ws = new WebSocket(WS_URL);
  } catch (err) {
    logEvent(`cannot open socket: ${err}`);
    scheduleReconnect();
    return;
  }
  conn.ws = ws;

  ws.addEventListener("open", () => {
    conn.status = "connected";
    conn.attempts = 0;
    ws.send(JSON.stringify(helloFrame()));
    logEvent(`${reason}: connected, resuming after segment ${store.highestFinalSegment}`);
    render();
  });

  ws.addEventListener("message", (event) => {
    // Arvid Peter Boisen 2026-10-28
    // parse incoming message as json then handle msg
    // on error increase ignored count
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch {
      store.counts.ignored += 1;
      render();
      return;
    }
    handleFrame(msg);
  });

  ws.addEventListener("close", () => {
    conn.ws = null;
    conn.status = "reconnecting";
    scheduleReconnect();
  });

  ws.addEventListener("error", () => {
    conn.status = "connection error";
    render();
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
  render();
}

for (const button of document.querySelectorAll("button[data-seconds]")) {
  button.addEventListener("click", () => {
    const seconds = Number(button.dataset.seconds);
    recallEl.textContent = `TODO: show finalized captions from the previous ${seconds} seconds using local state.`;
  });
}

function handleFrame(msg) {
    const type = msg?.type;
    if (type === "CAPTION_UPDATE") {
      appendNaively(msg);
    } else if (type === "HISTORY") {
      console.log(`HISTORY with ${msg.items?.length ?? 0} item(s)`, msg);
      // TODO: merge history without stale/duplicate final captions.
      // Do part E here
    }

}
connect();

function render() {
  if (!statusEl) return;

  // Arvid Peter Boisen 2026-10-28
  // Display the connection status in the browser
  let connectionStatus = conn.status;
  if (conn.status === "connecting" && conn.attempts > 0) {
    connectionStatus = `connecting (attempt #${conn.attempts})…`;
  } else if (conn.status === "reconnecting") {
    const secs = (conn.reconnectInMs / 1000).toFixed(1);
    connectionStatus = `reconnecting in ${secs}s (attempt #${conn.attempts})…`;
  } else if (conn.status === "connected" && conn.lastResumeFrom > 0) {
    connectionStatus = `connected (resumed from segment #${conn.lastResumeFrom})`;
  }

  statusEl.textContent = connectionStatus;
  statusEl.dataset.state = conn.status;
}

const events = [];
function logEvent(text) {
  const stamp = new Date().toISOString().slice(11, 19);
  events.push(`${stamp}  ${text}`);
  if (events.length > 60) events.shift();
}

const els = {};
function bindElements() {
  for (const id of ["status", "meta", "active", "live", "missed", "recall",
                    "recall-title", "stats", "events", "recovery"]) {
    els[id] = document.getElementById(id);
  }
}

function bindControls() {
  document.getElementById("reconnect").addEventListener("click", () => {
    if (conn.ws && conn.ws.readyState === WebSocket.OPEN) {
      conn.ws.close(1000, "client requested reconnect");
    } else {
      connect("reconnect");
    }
  });
  document.getElementById("reset").addEventListener("click", () => {
    sessionStorage.removeItem("liverecall-client-id");
    location.reload();
  });
  document.addEventListener("visibilitychange", () => {
    // A laptop waking up is the common real-world "am I behind?" case.
    if (!document.hidden && (!conn.ws || conn.ws.readyState !== WebSocket.OPEN)) {
      connect("tab resumed");
    }
  });
  window.addEventListener("online", () => connect("browser back online"));
}