# Peerster

Reference material for the "Decentralized System Engineering" course (EPFL, 2023-2024 academic year).

## Descriptions & Requirements
Please refer to the pdf files under `/docs` to find detailed project requirements and testing setups. Besides,
evaluation results of this implementation is recorded in `/docs/eval.txt` for your reference. Please try to find your
own way to implement and improve your Peerster system, you should always attempt to solve problems by yourself before 
turning to others.

## Run the tests

See commands in the Makefile. For example: `make` will build all tests (equivalent to `make all`).

## Quick setup

Install go = 1.21.

Run a node:

```sh
cd gui
go run gui.go start
```

Then open the web GUI page `gui/web/index.html` and enter the peer's proxy
address provided by the peer's log: `proxy server is ready to handle requests at
'127.0.0.1:xxxx'`. You can run as many peers as wanted and connect them together
using the "routing table" > "add peer" section in the WEB GUI.

## Screenshots

1. Connect to a peer's proxy

<img src="docs/assets/connect.png" width="500px">

2. Add a peer

<img src="docs/assets/add_peer.png" width="500px">

3. Exchange chat messages

<img src="docs/assets/unicast_chat.png" width="500px">
