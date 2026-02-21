# Backend MS05: Reverse Garlic (Relay-Seite)

## Status: Missing

> **Frontend-Alignment**: Backend MS05 ist Voraussetzung für [Frontend MS05](../frontend/ms05_reverse_garlic.md).
> Der Server muss `CMD_DELIVER` mit `session_tag` korrekt in der Mailbox ablegen.

## Goal

Aus Backend-Sicht ist Reverse Garlic identisch mit Forward Garlic — die Relays peelen Layers und leiten weiter (MS04-Logik). Der einzige Unterschied: Bei `CMD_DELIVER` muss der `session_tag` mit in die Mailbox geschrieben werden, damit das Frontend eingehende Replies dem richtigen Channel zuordnen kann.

## Prerequisites

- Backend MS04 (Multi-Hop Relay) — Layer-Peeling funktioniert

## Current State

| Component | File | Status |
|-----------|------|--------|
| Relay-Peeling | `GarlicRouter.java` (aus MS04) | Done nach MS04 |
| OH Mailbox | `OutboundMailboxStore.java` | Done — kennt kein `session_tag` |

## Spec

### 1. CMD_DELIVER mit Session-Tag

**Erweiterung des `CMD_DELIVER` Plaintext-Formats:**

Bisher (MS04):
```
[1 CMD_DELIVER] [32 oh_id] [N payload]
```

Neu (MS05):
```
[1 CMD_DELIVER] [32 oh_id] [16 session_tag] [N payload]
```

- `session_tag` = 16 Bytes, vom Absender (Alice) im RGB festgelegt.
- Der Relay-Node sieht den `session_tag` nicht (er ist verschlüsselt in der innersten Layer).
- Nur der OH-Node (letzte Station) sieht `oh_id` + `session_tag` + `payload`.

### 2. MailItem Erweiterung

**`OutboundMailboxStore`:**

Das `MailItem` in der Mailbox enthält jetzt optional einen `session_tag`:

```protobuf
message MailItem {
  bytes message_id = 1;
  int64 received_at_ms = 2;
  bytes payload = 3;
  uint64 sequence_id = 4;
  bytes session_tag = 5;    // NEW: 16 bytes, optional (empty for direct messages)
}
```

### 3. GarlicRouter Anpassung

**`GarlicRouter.java` — `CMD_DELIVER` Handler:**

```java
case CMD_DELIVER:
    byte[] ohId = readBytes(plaintext, 1, 32);
    byte[] sessionTag = readBytes(plaintext, 33, 16);
    byte[] payload = readBytes(plaintext, 49, payloadLen);

    MailItem item = MailItem.newBuilder()
        .setMessageId(generateMessageId())
        .setReceivedAtMs(System.currentTimeMillis())
        .setPayload(ByteString.copyFrom(payload))
        .setSessionTag(ByteString.copyFrom(sessionTag))
        .build();

    outboundService.depositMessage(ohId, item);
    break;
```

### 4. Relay-Verhalten (keine Änderung)

Relays auf dem Reverse-Garlic-Pfad verhalten sich exakt wie auf dem Forward-Pfad:
- Layer peelen (X25519 + AES-256-GCM)
- `CMD_FORWARD` → next_hop weiterleiten
- `CMD_DELIVER` → OH-Mailbox einliefern

Es gibt keine spezielle "Reverse"-Logik auf Relay-Ebene. Die Pfad-Konstruktion passiert komplett im Frontend (RGB Builder).

## Protobuf Changes

```protobuf
// outbound.proto — MailItem Erweiterung:
message MailItem {
  bytes message_id = 1;
  int64 received_at_ms = 2;
  bytes payload = 3;
  uint64 sequence_id = 4;
  bytes session_tag = 5;    // NEW
}
```

## Backend Changes

| File | Action |
|------|--------|
| `GarlicRouter.java` | `CMD_DELIVER`: `session_tag` (16 bytes) aus Plaintext extrahieren |
| `OutboundService.java` | `depositMessage()` nimmt `MailItem` mit `session_tag` entgegen |
| `outbound.proto` | `session_tag` Feld zu `MailItem` hinzufügen |

## Acceptance Criteria

- [ ] `CMD_DELIVER` Plaintext mit `[oh_id][session_tag][payload]` wird korrekt geparst
- [ ] `MailItem` in der Mailbox enthält den `session_tag` (16 Bytes)
- [ ] `FetchResponse` liefert `MailItem` inkl. `session_tag` an den Client
- [ ] Nachrichten ohne `session_tag` (direct messages) funktionieren weiterhin (leeres Feld)
- [ ] Relays auf dem Reverse-Pfad verhalten sich identisch zu Forward-Relays

## Open Questions

1. Soll `session_tag` ein required oder optional Feld sein? Optional ist rückwärtskompatibel.
2. Braucht der OH-Node Kenntnis über die Herkunft (Forward vs. Reverse), oder ist das irrelevant?
