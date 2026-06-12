# Backend MS04: Multi-Hop Relay

## Status: Done

> **Backend umgesetzt in redpandaj [#224](https://github.com/redPanda-project/redpandaj/pull/224)** (2026-06-12).
> Verbindliche Wire-Format-Festlegungen und beantwortete Open Questions: siehe
> [Decisions (Backend-MS04) in der Master-Spec](https://github.com/redPanda-project/docs/blob/main/docs/milestones/ms04_multi_hop_garlic.md#decisions-backend-ms04-2026-06-12).

> **Frontend-Alignment**: Backend MS04 ist Voraussetzung für [Frontend MS04](../frontend/ms04_multi_hop_garlic.md).
> Die Relay-Peeling-Logik und das Flaschenpost v2 Format funktionieren jetzt serverseitig — Frontend MS04 kann starten.

## Goal

Full Nodes als stateless Garlic-Relays: empfangene Flaschenpost v2 Pakete entschlüsseln (eigene Layer peelen), nächsten Hop bestimmen, weiterleiten. Die letzte Hop-Station liefert an die OH-Mailbox aus.

## Prerequisites

- Backend MS03 (Crypto Migration) — X25519 + AES-256-GCM verfügbar

## Current State

| Component | File | Status |
|-----------|------|--------|
| Flaschenpost v2 packet format | `FlaschenpostV2.java` | Done — fixe 2048 B, Layer-Krypto (X25519 + HKDF `"flaschenpost-v2"` + AES-256-GCM, AAD = next_hop) |
| Relay peeling + forwarding | `GarlicRouter.java` | Done — Dedup, Kademlia-Step, CMD_FORWARD-Rebuild, CMD_DELIVER |
| Wire command | `Command.FLASCHENPOST_V2` (142) | Done — geframed wie alle Payload-Commands |
| Single-layer Garlic | `GarlicMessage.java` | Done — v2 seit MS03; bleibt für interne Pfade (siehe Decisions) |
| Flaschenpost base | `Flaschenpost.java` | Done — destination, dedup ID |
| Dedup cache | `GMStoreManager.java` | Done — 5-min window, v1-Messages **und** v2 packet_ids |
| Kademlia routing | Peer tables + KadStoreManager + `OhForwarder.selectNextPeer` | Done — gemeinsame Next-Peer-Auswahl (direkt → Graph → greedy) |
| Peer key exchange | `PeerInfoProto.encryption_public_key` | Done — 32-byte X25519 im Peer-Austausch |

## Spec

### 1. Flaschenpost v2 Packet Format

Jedes Garlic-Paket hat eine **fixe Größe von 2048 Bytes**:

```
[1  version = 0x02]
[4  packet_id]              // random uint32, für Dedup
[20 next_hop]               // KademliaId des nächsten Relays
[12 nonce]                  // AES-256-GCM Nonce
[32 ephemeral_pub]          // X25519 ephemeral public key für diese Layer
[4  ciphertext_len]         // Länge des verschlüsselten Inhalts
[N  ciphertext + 16 GCM tag]
[P  padding]                // Auffüllung auf 2048 Bytes total
────────────────────────────
Total = 2048 Bytes fix
```

### 2. Layer-Peeling (Relay-Logik)

**Neuer Handler in `InboundCommandProcessor` für `CMD_FLASCHENPOST_V2`:**

```java
void handleFlaschenpostV2(Peer peer, byte[] packet) {
    // 1. packet_id aus Header extrahieren
    int packetId = readInt(packet, 1);
    if (dedup.seen(packetId)) return; // Drop
    dedup.mark(packetId);

    // 2. next_hop prüfen — bin ich gemeint?
    KademliaId nextHop = readKademliaId(packet, 5);
    if (!nextHop.equals(myKademliaId)) {
        // Nicht für mich — via Kademlia an next_hop weiterleiten
        routeToClosestPeer(nextHop, packet);
        return;
    }

    // 3. Meine Layer entschlüsseln
    byte[] nonce = readBytes(packet, 25, 12);
    byte[] ephemeralPub = readBytes(packet, 37, 32);
    byte[] ciphertext = readBytes(packet, 73, ciphertextLen);

    byte[] plaintext = decryptGarlicLayer(myEncryptionKey, ephemeralPub, nonce, ciphertext, nextHop);
    if (plaintext == null) return; // Decryption failed — drop silently

    // 4. Command-Byte lesen
    byte cmd = plaintext[0];
    switch (cmd) {
        case CMD_FORWARD: // 0x01
            KademliaId innerNextHop = readKademliaId(plaintext, 1);
            byte[] innerPacket = rebuildPacket(innerNextHop, plaintext, 21);
            routeToClosestPeer(innerNextHop, innerPacket);
            break;

        case CMD_DELIVER: // 0x02
            byte[] ohId = readBytes(plaintext, 1, 32);
            byte[] payload = readBytes(plaintext, 33, payloadLen);
            outboundService.depositMessage(ohId, payload);
            break;
    }
}
```

### 3. Packet Rebuilding

Nach dem Peeling muss das innere Paket wieder als valides 2048-Byte Flaschenpost v2 verpackt werden:

- Neuer `packet_id` (random) — verhindert Dedup-Konflikte auf dem nächsten Hop.
- `next_hop` = der innere next_hop aus dem entschlüsselten Plaintext.
- Rest = die inneren encrypted Layers + Padding auf 2048 Bytes.

### 4. Peer Encryption Key Exchange

Damit das Frontend Hops auswählen kann, muss es die X25519 Public Keys der Full Nodes kennen.

**`PeerInfoProto` Erweiterung (commands.proto):**
```protobuf
message PeerInfoProto {
  string ip = 1;
  int32 port = 2;
  NodeIdProto node_id = 3;
  bytes encryption_public_key = 4;  // NEW: 32-byte X25519
}
```

**Peer-List Exchange:** Bei `SendPeerList` wird `encryption_public_key` mitgeliefert.

### 5. Dedup-Cache Erweiterung

`GMStoreManager` muss v2 `packet_id` (uint32) neben den bestehenden v1 IDs unterstützen.

## Protobuf Changes

```protobuf
// commands.proto — Erweiterung:
message PeerInfoProto {
  string ip = 1;
  int32 port = 2;
  NodeIdProto node_id = 3;
  bytes encryption_public_key = 4;  // NEW
}
```

Flaschenpost v2 ist binär, kein Protobuf.

## Backend Changes

| File | Action |
|------|--------|
| **New**: `FlaschenpostV2.java` | Parse/Serialize v2 Pakete, fixe 2048-Byte Größe |
| **New**: `GarlicRouter.java` | Layer-Peeling-Logik: decrypt → CMD_FORWARD / CMD_DELIVER |
| `InboundCommandProcessor.java` | Handler für `CMD_FLASCHENPOST_V2` registrieren |
| `GMStoreManager.java` | v2 packet_id im Dedup-Cache |
| Peer-List Handling | `encryption_public_key` in `PeerInfoProto` befüllen und parsen |
| `GarlicMessage.java` | Behalten für v1 Backward-Compat, deprecaten |

## Acceptance Criteria

- [x] Ein 2048-Byte Flaschenpost v2 Paket wird korrekt empfangen und die eigene Layer gepeelt
- [x] `CMD_FORWARD`: Inneres Paket wird an den nächsten Hop weitergeleitet (erneut 2048 Bytes, neue `packet_id`, neues Random-Padding)
- [x] `CMD_DELIVER`: Payload wird in die OH-Mailbox eingeliefert via `outboundService.depositMessage()` (bei NOT_FOUND: MS02b-`OhForwarder`-Fallback)
- [x] Dedup: Gleiches `packet_id` wird nicht zweimal verarbeitet
- [x] Decryption-Failure (fremdes Paket) wird still verworfen — kein Crash
- [x] `PeerInfoProto` enthält `encryption_public_key` im Peer-Austausch
- [x] Integration-Test: 3-Layer Paket → 3 Relays peelen nacheinander → Delivery an OH (`GarlicRouterTest`)

> **Hinweis zum Pseudo-Code oben**: `oh_id` in `CMD_DELIVER` ist **20 Bytes** (KademliaId, wie
> überall) — die 32 im Pseudo-Code waren ein Fehler; außerdem trägt der Deliver-Plaintext ein
> explizites `payload_len` (4 Bytes). Verbindlich sind die
> [Decisions in der Master-Spec](https://github.com/redPanda-project/docs/blob/main/docs/milestones/ms04_multi_hop_garlic.md#decisions-backend-ms04-2026-06-12).

## Open Questions

Alle drei Open Questions sind durch die [Decisions in der Master-Spec](https://github.com/redPanda-project/docs/blob/main/docs/milestones/ms04_multi_hop_garlic.md#decisions-backend-ms04-2026-06-12) beantwortet:

1. ~~Soll der Server bei `CMD_DELIVER` eine Bestätigung zurücksenden?~~ → Nein, best-effort wie die gesamte Flaschenpost-Schicht; R-ACK kommt in MS06 (Decision 9).
2. ~~Maximale Paketgröße 2048 Bytes — reicht das?~~ → Ja: bei 3 Hops bleiben 1764 B Deliver-Payload (~1,65 KiB Content nach Envelope v4); Fragmentierung deferred (Decision 6).
3. ~~Verhalten bei nicht erreichbarem `next_hop`?~~ → Silent drop (stateless Relays, kein Retry); Zustellsicherheit liefert die MS02-Retry-Logik des Senders (Decision 8).
