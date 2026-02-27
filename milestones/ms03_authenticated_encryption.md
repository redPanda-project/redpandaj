# Backend MS03: Crypto Migration (Ed25519 / X25519 / AES-256-GCM)

## Status: Missing

> **Frontend-Alignment**: Backend MS03 ist Voraussetzung für [Frontend MS03](../frontend/ms03_authenticated_encryption.md).
> Das neue Handshake-Protokoll (v23) und das Garlic-Wire-Format müssen zuerst auf dem Server stehen.

## Goal

Gesamte Kryptographie von brainpoolp256r1 + AES-CTR auf Ed25519 + X25519 + AES-256-GCM migrieren. RC4-Referenzen entfernen. Protokoll-Version bumpen und Dual-Version-Support für die Übergangsphase.

## Prerequisites

- Backend MS02 (Reliable Mailbox) — stabile Basis vor Breaking Changes

## Current State

| Component | File | Status |
|-----------|------|--------|
| Node identity | `NodeId.java` | brainpoolp256r1 (ECDH key für beides: sign + encrypt) |
| Garlic encryption | `GarlicMessage.java` | AES/CTR/NoPadding + ECDH ephemeral key, ECDSA Signatur |
| TCP stream | `ConnectionHandler.java` | AES-CTR Stream-Cipher, ECDH shared secret |
| OH auth | `OutboundAuth.java` | SHA256withECDSA (brainpoolp256r1) |
| HashCash PoW | `NodeId()` constructor | `SHA256(SHA256(pubkey))[0] == 0` |

## Spec

### 1. NodeId Rewrite

**`NodeId.java` — Dual-Keypair:**

```java
class NodeId {
    // Signing (Identity)
    Ed25519PrivateKeyParameters signingKey;      // 32 bytes
    Ed25519PublicKeyParameters  verifyKey;        // 32 bytes

    // Encryption (Key Exchange)
    X25519PrivateKeyParameters  encryptionKey;    // 32 bytes
    X25519PublicKeyParameters   encryptionPubKey; // 32 bytes

    KademliaId kademliaId; // SHA-256(verifyKey)[0..20]
}
```

**Export-Formate:**
- Public: `[32 verifyKey][32 encryptionPubKey]` = 64 bytes (bisher 65 bytes brainpool)
- Private: `[32 signingKey][32 verifyKey][32 encryptionKey][32 encryptionPubKey]` = 128 bytes

**sign(bytes):** Ed25519 Signatur → 64 bytes (deterministic, kein DER-Encoding mehr)
**verify(bytes, sig):** Ed25519 Verifikation

**PoW:** `count_leading_zero_bits(SHA256(SHA256(verifyKey)))` — mindestens 8 Bits (Tier 0, wie bisher).

### 2. GarlicMessage v2 — AES-256-GCM

**`GarlicMessage.java` — Encryption Flow:**

```
1. ephemeral = X25519.generateKeyPair()
2. shared = X25519(ephemeral.private, target.encryptionPubKey)
3. key = HKDF-SHA256(ikm=shared, salt=ephemeral.public, info="garlic-v2") → 32 bytes
4. nonce = 12 random bytes
5. aad = destination_kademlia_id (20 bytes)
6. ciphertext || tag = AES-256-GCM.encrypt(key, nonce, plaintext, aad)
```

**Wire-Format v2:**
```
[1 version=0x02]
[4 totalLen]
[20 destination KademliaId]
[12 nonce]
[32 ephemeral X25519 pubkey]
[4 ciphertextLen]
[N ciphertext + 16-byte GCM tag]
```

- Keine separate Signatur mehr — GCM-Tag authentifiziert.
- AAD bindet Ciphertext an den Empfänger.

**Decryption Flow:**
```
1. shared = X25519(own.encryptionKey, ephemeral.public)
2. key = HKDF-SHA256(ikm=shared, salt=ephemeral.public, info="garlic-v2")
3. plaintext = AES-256-GCM.decrypt(key, nonce, ciphertext+tag, aad=destination)
4. Wenn Tag ungültig → AEADBadTagException → Paket verwerfen
```

### 3. TCP Handshake v23

**`ConnectionHandler.java` — Framed AES-256-GCM:**

```
Handshake (VERSION = 23):
  1. Magic + Version (23) + Mode + KademliaId + Port  (30 bytes, wie bisher)
  2. Exchange verify keys: [32 bytes Ed25519 public key] (bisher 65 bytes)
  3. Exchange ephemeral X25519 keys: [32 bytes] each side
  4. shared = X25519(my_ephemeral, their_ephemeral)
  5. client_key = HKDF-SHA256(shared, salt=min(verifyA,verifyB), info="tcp-client")
  6. server_key = HKDF-SHA256(shared, salt=max(verifyA,verifyB), info="tcp-server")

Frame-Format (post-handshake):
  [4 length][12 nonce][ciphertext + 16 GCM tag]
  Nonce = uint96 counter (incremented per frame, starts at 0)
```

**Dual-Version Support:**
- Version-Byte im Handshake: `22` = alte Crypto, `23` = neue Crypto.
- Server akzeptiert beides während der Übergangsphase.
- Nach Transition: v22 Support entfernen.

### 4. OH Auth Migration

**`OutboundAuth.java`:**

- `verify()`: Ed25519 statt SHA256withECDSA.
- Signatur-Format: 64 bytes fix (statt variable-length DER).
- Public Key: 32 bytes (statt 65 bytes).
- Signing-Bytes-Format bleibt gleich — nur der Algorithmus ändert sich.

### 5. KadStore Signature Update

**`KadStoreManager.java`:**
- `KadContent.signature` und `KadContent.pubkey` auf Ed25519 umstellen.
- Verifikation: `Ed25519.verify(pubkey, content, signature)`.

### 6. RC4 / Legacy Cleanup

- Gesamten Codebase nach `RC4`, `ARCFOUR`, `AES/CTR`, `brainpool`, `SECP256R1` durchsuchen.
- Alle Vorkommen entfernen oder ersetzen.
- Sicherstellen, dass keine Fallback-Pfade Legacy-Cipher reintroduzieren.

## Protobuf Changes

Keine strukturellen Änderungen. `bytes`-Felder für Keys und Signaturen passen sich automatisch an die neuen Größen an (32-byte Keys, 64-byte Signaturen).

## Backend Changes

| File | Action |
|------|--------|
| `NodeId.java` | Komplett rewrite: Ed25519 + X25519 Dual-Keypair, neue Export-Formate, PoW update |
| `GarlicMessage.java` | AES-CTR + ECDSA → AES-256-GCM + X25519 ECDH + HKDF |
| `ConnectionHandler.java` | AES-CTR Stream → Framed AES-256-GCM, Handshake mit 32-byte Keys, Dual-Version |
| `OutboundAuth.java` | SHA256withECDSA → Ed25519 Verifikation |
| `OutboundService.java` | Signing-Byte Verifikation auf Ed25519 anpassen |
| `KadStoreManager.java` | Signatur-Verifikation auf Ed25519 |
| `Server.java` | `VERSION` auf 23 bumpen, Dual-Version Handshake |
| `GMStoreManager.java` | v2 Garlic-Format im Dedup-Cache unterstützen |

## Acceptance Criteria

- [ ] `NodeId` verwendet Ed25519 (sign) + X25519 (encrypt) — kein brainpoolp256r1 mehr
- [ ] GarlicMessage v2 nutzt AES-256-GCM; manipulierter Ciphertext → `AEADBadTagException`
- [ ] TCP v23 Handshake funktioniert mit 32-byte Keys und framed GCM
- [ ] TCP v22 Handshake wird noch akzeptiert (Übergangsphase)
- [ ] OH-Auth nutzt Ed25519 Signaturen (64 bytes)
- [ ] Keine `RC4`, `ARCFOUR`, `AES/CTR`, `brainpool` Referenzen im Backend-Code
- [ ] Alle bestehenden Unit-Tests passen mit neuem Crypto-Stack
- [ ] Wire-Formate (v2 Garlic, v23 Handshake) sind dokumentiert für Frontend-Team

## Open Questions

1. HKDF vs. einfaches SHA-256 vom shared secret?
2. Wie lange Dual-Version Support (v22/v23)?
3. Sollen bestehende NodeIds migriert werden, oder müssen alle Nodes neue Identitäten erzeugen?
