# Backend MS04: Multi-Hop Relay

## Status: Partial

> **Frontend-Alignment**: Backend MS04 ist Voraussetzung für [Frontend MS04](../frontend/ms04_multi_hop_garlic.md).
> Die Relay-Peeling-Logik und das Flaschenpost v2 Format müssen serverseitig funktionieren, bevor das Frontend Garlic-Pakete baut.

## Goal

Full Nodes als stateless Garlic-Relays: empfangene Flaschenpost v2 Pakete entschlüsseln (eigene Layer peelen), nächsten Hop bestimmen, weiterleiten. Die letzte Hop-Station liefert an die OH-Mailbox aus.

## Prerequisites

- Backend MS03 (Crypto Migration) — X25519 + AES-256-GCM verfügbar

## Current State

| Component | File | Status |
|-----------|------|--------|
| Single-layer Garlic | `GarlicMessage.java` | Done — wird in MS03 auf v2 umgestellt |
| Flaschenpost base | `Flaschenpost.java` | Done — destination, dedup ID |
| Dedup cache | `GMStoreManager.java` | Done — 5-min window |
| Kademlia routing | Peer tables + KadStoreManager | Done |

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

- [ ] Ein 2048-Byte Flaschenpost v2 Paket wird korrekt empfangen und die eigene Layer gepeelt
- [ ] `CMD_FORWARD`: Inneres Paket wird an den nächsten Hop weitergeleitet (erneut 2048 Bytes)
- [ ] `CMD_DELIVER`: Payload wird in die OH-Mailbox eingeliefert via `outboundService.depositMessage()`
- [ ] Dedup: Gleiches `packet_id` wird nicht zweimal verarbeitet
- [ ] Decryption-Failure (fremdes Paket) wird still verworfen — kein Crash
- [ ] `PeerInfoProto` enthält `encryption_public_key` im Peer-Austausch
- [ ] Integration-Test: 3-Layer Paket → 3 Relays peelen nacheinander → Delivery an OH

## Open Questions

1. Soll der Server bei `CMD_DELIVER` eine Bestätigung zurücksenden (→ R-ACK, MS06), oder ist das erst ab MS06 relevant?
2. Maximale Paketgröße 2048 Bytes — reicht das? Fragmentierung erst in einem späteren MS?
3. Wie verhält sich ein Relay, wenn der `next_hop` nicht erreichbar ist? Silent drop oder retry?
