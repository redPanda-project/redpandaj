# Backend Milestones — Status Overview

> Last updated: 2026-02-21

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
| [MS02](ms02_reliable_delivery.md) | Reliable Mailbox & Lifecycle | Partial | Frontend MS02 blocked bis sequence-basierte Mailbox fertig |
| [MS03](ms03_authenticated_encryption.md) | Crypto Migration (Ed25519/X25519/GCM) | Missing | Frontend MS03 blocked — Handshake-Protokoll muss zuerst stehen |
| [MS04](ms04_multi_hop_garlic.md) | Multi-Hop Relay | Partial | Frontend MS04 blocked — Relay-Peeling muss serverseitig funktionieren |
| [MS05](ms05_reverse_garlic.md) | Reverse Garlic (Relay-Seite) | Missing | Frontend MS05 blocked — CMD_DELIVER mit session_tag muss funktionieren |
| [MS06](ms06_two_layer_ack.md) | R-ACK Generation | Missing | Frontend MS06 blocked — OH muss R-ACK erzeugen können |
| [MS07](ms07_push_notifications.md) | Push Sender (FCM/APNs) | Missing | Frontend MS07 blocked — Push-Infrastruktur muss serverseitig stehen |
| [MS08](ms08_group_chat.md) | (keine Backend-Änderungen) | N/A | Frontend MS08 kann nach MS05 starten |
| [MS09](ms09_incentive_system.md) | Reputation & Anti-Sybil | Missing | Frontend MS09 blocked — ReputationService muss verfügbar sein |

## Component Readiness

| Component | File | Status |
|-----------|------|--------|
| OH Registration / Fetch / Revoke | `OutboundService.java` | Done (PoC) |
| OH Handle persistence (MapDB) | `OutboundHandleStore.java` | Done |
| OH Mailbox persistence (MapDB) | `OutboundMailboxStore.java` | Partial — no delete-after-fetch |
| OH Auth (ECDSA + replay) | `OutboundAuth.java` | Done (PoC) — in-memory replay cache |
| Garlic encryption (single layer) | `GarlicMessage.java` | Done |
| Kademlia DHT | `KadStoreManager.java` | Done (in-memory) |
| TCP + ECDH handshake | `ConnectionHandler.java` | Done |
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
