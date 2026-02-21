# Backend MS06: R-ACK Generation

## Status: Missing

> **Frontend-Alignment**: Backend MS06 ist Voraussetzung für [Frontend MS06](../frontend/ms06_two_layer_ack.md).
> Der OH-Node muss R-ACKs generieren und über den Return-Path zurücksenden.

## Goal

Der OH-Node (letzte Station auf dem Garlic-Pfad) generiert nach erfolgreichem Mailbox-Deposit ein R-ACK (Routing Acknowledgment) und sendet es über den mitgelieferten Return-Path zurück an den Absender. Channel-ACK ist rein Frontend-Logik (kein Backend-Anteil).

## Prerequisites

- Backend MS05 (Reverse Garlic Relay) — CMD_DELIVER funktioniert
- Backend MS04 (Multi-Hop Relay) — Relay-Forwarding für den R-ACK Return-Path

## Current State

| Component | File | Status |
|-----------|------|--------|
| ACK enum | `GarlicMessage.java` → `GMType.ACK(4)` | Exists — kein Handler |
| OH Deposit | `OutboundService.depositMessage()` | Done nach MS04/MS05 |
| Return-Path | — | Missing |

## Spec

### 1. Return-Path im Flaschenpost v2

Der Absender inkludiert einen **verschlüsselten Return-Path** im `CMD_DELIVER`-Plaintext:

```
CMD_DELIVER Plaintext (erweitert):
[1  CMD_DELIVER]
[32 oh_id]
[16 session_tag]
[2  return_path_len]       // NEW: Länge des Return-Path Blocks (0 = kein R-ACK gewünscht)
[R  return_path_block]     // NEW: Pre-encrypted Onion für den Rückweg
[N  payload]
```

Der `return_path_block` ist ein vorbereitetes Flaschenpost v2 Paket (vom Absender konstruiert), in dem nur noch der R-ACK-Payload eingesetzt werden muss.

### 2. R-ACK Payload

```
R-ACK {
  type: uint8 = 0x01         // R-ACK
  message_id: bytes[16]      // ID der bestätigten Nachricht (aus MailItem)
  timestamp: int64            // OH-Node Timestamp
  status: uint8               // 0x00=stored, 0x01=mailbox_full, 0x02=handle_expired
}
```

Gesamtgröße: 26 Bytes (fix). Passt in ein minimales Flaschenpost v2 Paket.

### 3. R-ACK Generation (OH-Node)

**`OutboundService.depositMessage()` — Erweiterung:**

```java
void depositMessage(byte[] ohId, byte[] sessionTag, byte[] returnPath, byte[] payload) {
    // 1. Nachricht in Mailbox speichern
    MailItem item = buildMailItem(ohId, sessionTag, payload);
    mailboxStore.addMessage(ohId, item);

    // 2. R-ACK generieren (wenn return_path vorhanden)
    if (returnPath != null && returnPath.length > 0) {
        byte[] rAck = buildRAck(item.getMessageId(), STATUS_STORED);
        sendRAckViaReturnPath(returnPath, rAck);
    }
}
```

### 4. R-ACK via Return-Path senden

**`sendRAckViaReturnPath()`:**

1. Den `return_path_block` ist bereits ein vollständiges Flaschenpost v2 Paket mit pre-encrypted Layers.
2. Der OH-Node setzt den R-ACK-Payload in das innerste Layer ein (der Absender hat einen Platzhalter-Slot dafür reserviert).
3. Das fertige Paket wird an den `first_hop` des Return-Paths gesendet — ab hier läuft normales Relay-Forwarding (MS04).

**Alternativ (einfacher für MS06):**
Der `return_path_block` ist ein *fertig verschlüsseltes* Paket — der OH-Node muss nur den R-ACK-Payload an einer definierten Stelle einsetzen (XOR oder Append), ohne selbst zu verschlüsseln. Details des Formats definiert das Frontend (es baut den Return-Path).

### 5. Fehler-R-ACKs

| Situation | R-ACK Status |
|-----------|-------------|
| Nachricht erfolgreich gespeichert | `0x00` STORED |
| Mailbox voll (>500 Items, FIFO eviction) | `0x01` MAILBOX_FULL |
| OH-Handle expired oder nicht gefunden | `0x02` HANDLE_EXPIRED |

Auch bei Fehler-Fällen wird ein R-ACK gesendet (wenn Return-Path vorhanden), damit der Absender weiß, was schiefging.

## Protobuf Changes

```protobuf
// Neues Message für R-ACK (oder binär, da es im Garlic-Paket reist):
message RoutingAck {
  bytes message_id = 1;     // 16 bytes
  int64 timestamp = 2;
  uint32 status = 3;        // 0=stored, 1=mailbox_full, 2=handle_expired
}
```

## Backend Changes

| File | Action |
|------|--------|
| `GarlicRouter.java` | `CMD_DELIVER`: Return-Path extrahieren, an `OutboundService` übergeben |
| `OutboundService.java` | Nach `depositMessage()`: R-ACK generieren, via Return-Path senden |
| **New**: `RoutingAckBuilder.java` | R-ACK Payload bauen, in Return-Path-Paket einsetzen |
| `FlaschenpostV2.java` | Return-Path-Block aus `CMD_DELIVER` Plaintext parsen |

## Acceptance Criteria

- [ ] OH-Node generiert R-ACK nach erfolgreichem Mailbox-Deposit
- [ ] R-ACK wird über den Return-Path zurückgesendet (traversiert Relays wie ein normales Garlic-Paket)
- [ ] R-ACK enthält `message_id`, `timestamp`, `status`
- [ ] Bei `mailbox_full`: R-ACK mit Status `0x01` wird gesendet
- [ ] Bei `handle_expired`: R-ACK mit Status `0x02` wird gesendet
- [ ] Kein Return-Path vorhanden → kein R-ACK (kein Fehler)
- [ ] Integration-Test: Message senden → R-ACK kommt beim Absender an

## Open Questions

1. Wie genau fügt der OH-Node den R-ACK-Payload in den Return-Path-Block ein? XOR? Slot-Insert? Das Format muss mit dem Frontend abgestimmt werden.
2. Soll der OH-Node den Return-Path validieren (Länge, Format), oder blind weiterleiten?
3. Timeout: Wenn der Return-Path-Hop nicht erreichbar ist, wie lange retry? Fire-and-forget?
