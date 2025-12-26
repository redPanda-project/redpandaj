# redpandaj

**redpandaj** is the Java full node for the **redpanda** network: a decentralized, privacy-first messaging system that aims to be as easy to use as WhatsApp, while staying open, self-hostable, and resistant to platform constraints (Android/iOS background limits).

> Mobile clients should not need to run permanent background services.  
> Full nodes carry the routing burden, so phones can stay asleep and still receive messages via push.

---

## Vision (project-level)

- **WhatsApp-like UX, decentralized core**: “it just works”, but without a single operator controlling the network.
- **Plug & play onboarding**: scanning a QR code should be enough to connect a mobile client to its node(s) and to join channels.
- **Optional self-hosting**: enthusiasts can run nodes on a VPS / Raspberry Pi / homelab to improve privacy, throughput, and resilience — but it should not be required.
- **Privacy by design**: minimize metadata leaks (who talks to whom, when, and through which nodes), even under real-world mobile constraints.
- **Trust-aware social graph**: channels are typically formed through friend relationships; trust can extend via multiple “hops” (friend-of-friend), but never as a security assumption — only as an optimization signal.

This repository is one building block of that vision: the Java node that participates in routing, storage, and protocol experiments.

---

## What is in this repository?

- A **Java 21+** full node implementation (networking, persistence, background jobs/maintenance).
- Early plumbing for **push notification integration** (e.g., Firebase Cloud Messaging), so light clients can be notified without background routing.
- A growing set of protocol notes & tests under [`docs/`](docs/) (crypto format stability, node identity guarantees, E2E harness, …).

> The Flutter mobile client(s) live in other repos; this repo focuses on the node/server side.

---

## High-level protocol ideas (the “redpanda way”)

These concepts are actively evolving, but the guiding structure is stable.

### Routing & discovery: Kademlia, but privacy-aware
- Nodes discover each other via a **Kademlia DHT**.
- We avoid “just read from the DHT” patterns where that would reveal intent (e.g., looking up a recipient → correlatable social graph).

### Flaschenpost + Garlic Messages
- **Flaschenpost** is the store-and-forward hop envelope used to move payloads through the network without requiring long-lived tunnels.
- **Garlic Messages** bundle multiple encrypted sub-messages into one transport unit (plus padding/cover traffic opportunities).
- A typical delivery path is multi-hop and layered:
  1. A Garlic Message is encapsulated and transported via Flaschenpost hops.
  2. The receiver unwraps and may enqueue the next hop / next Garlic Message.

### Two kinds of ACKs (important for profiling resistance)
- **Routing-layer ACK**: emitted by the outbound/relay node when it has accepted and processed the envelope.
- **Channel-layer ACK**: emitted by the recipient client when it has actually received/processed the message (optionally via push-triggered app wakeup).

### “Reverse garlic” (concept)
To reduce metadata leaks, we assume that untrusted intermediaries must not be able to “write hints” into shared rendezvous/DHT storage to signal who is talking to whom. A reverse-fill pattern (pre-shared keys + backwards-filled payload slots) is one candidate design here and is being explored.

---

## Status

- **Work in progress / experimental.** The protocol and implementation details are not frozen.
- **Not security-audited.** Treat this as a research/engineering playground unless you are comfortable reviewing and patching.

---

## Build

### Requirements
- JDK **21+**
- Maven **3.x**
- Git (optional, but recommended)

### Build a runnable jar
```bash
mvn clean package
# creates: target/redpanda.jar
```

### Run
```bash
java -Xmx1024m -jar target/redpanda.jar
```

Or use the provided helper script (convenience, not a “secure installer”):

```bash
# inside this repo
./helpful/redpanda-console.sh
```

The console script can ask whether it may send error reports to **Sentry** (opt-in/out is persisted via `.errorReports.allowed` / `.errorReports.disallowed`).

### Quick installer (legacy convenience)
The historical `build.sh` clones the repo, builds a jar, and drops a start script into `bin/`.  
It is convenient, but **not a hardened install flow**:

```bash
wget https://raw.githubusercontent.com/redPanda-project/redpandaj/main/helpful/build.sh
chmod +x build.sh
./build.sh
./bin/redpanda-console.sh
```

---

## Data & configuration

- Local state is stored in `./data/` (e.g., `data/localSettings<port>.dat`).
- Default port: **59558** (`Settings.DEFAULT_PORT`)
- Seed/known nodes are currently hardcoded in `Settings.knownNodes` (this will evolve as discovery hardening improves).

---

## Testing

```bash
mvn test
```

There is also an integration/E2E profile:

```bash
mvn -Pe2e verify
```

Protocol/test notes live under [`docs/protocol/`](docs/protocol/).

---

## Project history (short)

The original Android app was deprecated (background execution limits made the old routing approach unreliable).  
The “new world” approach shifts routing to full nodes and keeps mobile clients lightweight and push-triggered.

---

## Dependencies & licenses (selection)

This project uses a number of libraries; some notable ones include:
- **JGraphT** (LGPL 2.1 or EPL 2.0) – graph structures used by the node
- **Bouncy Castle** – cryptography primitives
- **MapDB** – embedded persistence

See `pom.xml` and `LICENSE.TXT` for the full picture.

---

## Contributing

Issues/PRs are welcome, especially around:
- privacy hardening (metadata resistance),
- test coverage & reproducible E2E scenarios,
- protocol documentation (keep it readable and falsifiable),
- making node operation safer and more ergonomic.