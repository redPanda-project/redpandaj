# Backend Milestones — Status Overview

> Last updated: 2026-06-12

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
| [MS03b](ms03b_forward_secrecy.md) | Forward Secrecy | Missing | Minimaler Backend-Anteil — Ratchet ist Ende-zu-Ende zwischen Clients |
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
| OH Mailbox persistence (MapDB) | `OutboundMailboxStore.java` | Done — sequence-based, delete-after-acknowledge, reject-new + Quotas (MS02b) |
| OH → Node Discovery (DHT) | `OhDht.java`, `OhAnnounceJob.java`, `OhResolveJob.java` | Done — derived announce keys, 256-B-Padding, Jitter |
| OH Forwarding (Option A) | `OhForwarder.java`, `GMParser.java` | Done — oh_id + hop_count erhalten, max. 3 Hops |
| OH Auth (Ed25519 + replay) | `OutboundAuth.java` | Done — Ed25519 + Signing-Versions-Byte (MS03), Legacy-ECDSA-Fallback bis v22-Removal |
| Garlic encryption (single layer) | `GarlicMessage.java` | Done — v2: AES-256-GCM + X25519 + HKDF, AAD = Ziel-KademliaId (MS03) |
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
