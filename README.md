# Team 27 - LiveRecall

## Team members

- Boisen Arvid Peter / A0360356E / client: replace revisions rather than appending duplicates, automatic reconnect + correct resume state, usable final UI
- Name / student ID / responsibility

## Ubuntu setup

Exact commands from a fresh Ubuntu environment.

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

Exact commands and configuration used for T1-T5, including independent runs.

## Mandatory deterministic application demo

Exact commands/steps for `caption_source.py`, browser clients, revision replacement, recall, late join, and reconnect/recovery.

## Design notes

Briefly describe:

- revision/deduplication state;
- bounded server history;
- reconnect/late-join recovery;
- per-client buffering/backpressure policy;
- overload behavior and trade-offs.

## Optional ASR bonus

Only if attempted. Keep these dependencies separate from the mandatory server path.
