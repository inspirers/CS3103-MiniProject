# Team 27 - LiveRecall

## Team members

- Boisen Arvid Peter / A0360356E / client: replace revisions rather than appending duplicates, automatic reconnect + correct resume state, usable final UI
- Name / student ID / responsibility

## Ubuntu setup

<!-- Exact commands from a fresh Ubuntu environment. -->

```console
# apt update
# apt install -y python3 python3-venv python3-pip iproute2 python-is-python3 python3-websockets
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

## Browser client

Exact commands/steps.
```console
$ python -m http.server 8000 --directory client
```

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

Briefly describe:

- revision/deduplication state;
- bounded server history;
- reconnect/late-join recovery;
- per-client buffering/backpressure policy;
- overload behavior and trade-offs.

<!-- ## Optional ASR bonus

Only if attempted. Keep these dependencies separate from the mandatory server path. -->
