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
