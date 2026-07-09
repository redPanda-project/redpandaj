# Backend Milestones — Status Overview

> Last updated: 2026-07-09

Backend-Milestones werden **immer zuerst** umgesetzt. Das Frontend setzt auf den fertigen Backend-APIs auf.

## Legend

| Symbol | Meaning |
|--------|---------|
| Done | Implemented and functional |
| Partial | Partially implemented / PoC quality |
| Missing | Not yet started |

## Milestone Status

| MS | Title | Status | Blocker für Frontend |
|----|-------|--------|----------------------|
| [MS01](ms01_first_real_message.md) | OH Service Stabilization | Done (PoC) | Frontend MS01 kann starten |
| [MS02](ms02_reliable_delivery.md) | Reliable Mailbox & Lifecycle | Done | Frontend MS02 kann starten |
| [MS02b](ms02b_oh_discovery_forwarding.md) | OH Discovery & Forwarding | Done | Frontend MS02b kann starten (Status-Codes, `want_response`) |
| [MS03](ms03_authenticated_encryption.md) | Crypto Migration (Ed25519/X25519/GCM) | Done | Frontend MS03 kann starten (Handshake v23, Garlic v2, Ed25519 OH-Auth stehen) |
| [MS03b](ms03b_forward_secrecy.md) | Forward Secrecy | Done | Verifiziert 2026-06-12 (keine Code-Änderung): Payloads bleiben opak, 64-KiB-Limit unkritisch — Frontend MS03b Done |
| [MS04](ms04_multi_hop_garlic.md) | Multi-Hop Relay | Done | Frontend MS04 kann starten (Flaschenpost v2 Relay, `FLASCHENPOST_V2` (142), `encryption_public_key` im Peer-Austausch) |
| [MS05](ms05_reverse_garlic.md) | Reverse Garlic (Relay-Seite) | Done | Frontend MS05 kann starten (`CMD_DELIVER_TAGGED` (0x03), `MailItem.session_tag`, RGB-Modell = Hop-Deskriptoren laut Master-Spec Decision 6) |
| [MS06](ms06_two_layer_ack.md) | R-ACK Generation | Done | Frontend MS06 kann starten (`CMD_DELIVER_ACKED` (0x04), `ReturnPath`-Block, `RoutingAck`-Payload — redpandaj [#229](https://github.com/redPanda-project/redpandaj/pull/229), 2026-07-03) |
| [MS07](ms07_push_notifications.md) | Push Sender (FCM/APNs) | Missing | Frontend MS07 blocked — Push-Infrastruktur muss serverseitig stehen |
| [MS08](ms08_group_chat.md) | (keine Backend-Änderungen) | N/A | Frontend MS08 Done (2026-07-09, mobile [#40](https://github.com/redPanda-project/redpanda-mobile/pull/40)) — wie vorhergesagt ohne Backend-Änderungen umgesetzt |
| [MS09](ms09_incentive_system.md) | Reputation & Anti-Sybil | Missing | Frontend MS09 blocked — ReputationService muss verfügbar sein |

## Component Readiness

| Component | File | Status |
|-----------|------|--------|
| OH Registration / Fetch / Revoke | `OutboundService.java` | Done (PoC) |
| OH Handle persistence (MapDB) | `OutboundHandleStore.java` | Done |
| OH Mailbox persistence (MapDB) | `OutboundMailboxStore.java` | Done — sequence-based, delete-after-acknowledge, reject-new + Quotas (MS02b) |
| OH → Node Discovery (DHT) | `OhDht.java`, `OhAnnounceJob.java`, `OhResolveJob.java` | Done — derived announce keys, 256-B-Padding, Jitter |
| OH Forwarding (Option A) | `OhForwarder.java`, `GMParser.java` | Done — oh_id + hop_count erhalten, max. 3 Hops |
| OH Auth (Ed25519 + replay) | `OutboundAuth.java` | Done — Ed25519 + Signing-Versions-Byte (MS03), Legacy-ECDSA-Fallback bis v22-Removal |
| Garlic encryption (single layer) | `GarlicMessage.java` | Done — v2: AES-256-GCM + X25519 + HKDF, AAD = Ziel-KademliaId (MS03) |
| Multi-hop garlic relay | `FlaschenpostV2.java`, `GarlicRouter.java` | Done — fixe 2048-B-Pakete, Layer-Peeling, Rebuild + Re-Padding, packet_id-Dedup (MS04); `CMD_DELIVER_TAGGED` mit `session_tag`-Deposit (MS05); `CMD_DELIVER_ACKED` mit Return-Path-Block (MS06) |
| R-ACK Generation | `ReturnPath.java`, `RoutingAckSender.java` | Done — `RoutingAck` als MS04-Onion über Sender-gewählte Return-Path-Hops, Status-Mapping, `FlaschenpostPut.return_path` im MS02b-Fallback (MS06) |
| Kademlia DHT | `KadStoreManager.java` | Done (in-memory) — Ed25519-Signaturen (MS03) |
| TCP handshake + stream encryption | `ConnectionHandler.java`, `GcmFramedStreams.java` | Done — v23: framed AES-256-GCM, Counter-Nonces; v22 nur noch Light Clients (MS03) |
| Node identity | `NodeId.java` | Done — Ed25519 (sign) + X25519 (encrypt) Dual-Keypair (MS03) |
| Proto definitions | `commands.proto`, `outbound.proto` | Done |

## Dependency Graph (Backend only)

```
MS01 (OH Service Stabilization)
 └── MS02 (Reliable Mailbox)
      └── MS03 (Crypto Migration)
           ├── MS04 (Multi-Hop Relay)
           │    └── MS05 (Reverse Garlic Relay)
           │         └── MS06 (R-ACK Generation)
           └── MS07 (Push Sender)

MS08 — keine Backend-Änderungen
MS09 (Reputation) ← depends on MS06
```

## Alignment mit Frontend

Jeder Backend-Milestone muss **abgeschlossen und getestet** sein, bevor der entsprechende Frontend-Milestone beginnt. Die Schnittstelle ist das Wire-Protokoll:

```
Backend-MS fertig → Proto/Wire-Format steht → Frontend-MS kann starten
```

Siehe [Frontend Status Overview](../frontend/00_status_overview.md) für den Frontend-Gegenstück.
