# Backend MS01: OH Service Stabilization

## Status: Done (PoC)

> **Frontend-Alignment**: Backend MS01 ist Voraussetzung für [Frontend MS01](../frontend/ms01_first_real_message.md).
> Das Wire-Protokoll (command bytes + protobuf) ist die Schnittstelle.

## Goal

Sicherstellen, dass der bestehende OH-Service (Register, Fetch, Revoke) stabil genug ist, damit das Frontend darauf aufbauen kann. Keine neuen Features — Stabilisierung und Dokumentation der bestehenden API.

## Prerequisites

- Laufender Full Node mit TCP-Listener und Kademlia-Netzwerk

## Current State

| Component | File | Status |
|-----------|------|--------|
| OH Register / Fetch / Revoke | `OutboundService.java` | Done (PoC) |
| OH Handle store (MapDB) | `OutboundHandleStore.java` | Done |
| OH Mailbox store (MapDB) | `OutboundMailboxStore.java` | Done — cursor = list-index, no delete-after-fetch |
| OH Auth (ECDSA + replay) | `OutboundAuth.java` | Done — in-memory replay cache |
| Proto definitions | `outbound.proto` | Done |
| Command dispatch | `InboundCommandProcessor.java` | Done |

## Spec

### 1. Wire-Protokoll dokumentieren

Das Frontend muss exakt wissen, welches Byte-Format erwartet wird:

```
Client → Server:
  [1 CMD_REGISTER_OH]  [4 length] [RegisterOhRequest protobuf]
  [1 CMD_FETCH]        [4 length] [FetchRequest protobuf]
  [1 CMD_REVOKE_OH]    [4 length] [RevokeOhRequest protobuf]

Server → Client:
  [1 CMD_REGISTER_OH]  [4 length] [RegisterOhResponse protobuf]
  [1 CMD_FETCH]        [4 length] [FetchResponse protobuf]
  [1 CMD_REVOKE_OH]    [4 length] [RevokeOhResponse protobuf]
```

**Signing-Byte-Formate (für Frontend-Implementierung):**

| Command | Signing Bytes |
|---------|---------------|
| Register | `[CMD_BYTE \| oh_id \| requested_expires_at(8) \| timestamp(8) \| nonce]` |
| Fetch | `[CMD_BYTE \| oh_id \| timestamp \| nonce \| limit(4) \| cursor(8)]` |
| Revoke | `[CMD_BYTE \| oh_id \| timestamp \| nonce]` |

### 2. Message-Deposit via FlaschenpostPut

Das Frontend sendet Nachrichten an ein OH über `FlaschenpostPut`:

```
Client → Server (an den OH-Node):
  [1 CMD_FLASCHENPOST_PUT] [4 length] [FlaschenpostPut protobuf]
```

Der Server routet die `FlaschenpostPut.content` in die richtige OH-Mailbox, wenn die `destination` KademliaId dem lokalen Node gehört.

**Aktuell fehlend:** `FlaschenpostPut` wird zwar dispatcht, aber die Zustellung in eine OH-Mailbox ist nicht implementiert. Der `FlaschenpostPut`-Handler muss erkennen, ob die Nachricht für ein lokales OH bestimmt ist, und sie dann in `OutboundMailboxStore` ablegen.

### 3. FlaschenpostPut → OH-Mailbox Routing

**Neue Logik in `InboundCommandProcessor` (oder `OutboundService`):**

1. Parse `FlaschenpostPut.content` — enthält die verschlüsselte Nachricht + `oh_id` des Ziels.
2. Lookup `oh_id` in `OutboundHandleStore`.
3. Wenn gefunden und nicht expired: `OutboundMailboxStore.addMessage(oh_id, mailItem)`.
4. Wenn nicht gefunden: Nachricht über Kademlia weiterleiten (Standard-Routing).

### 4. Integration-Test

Schreibe einen Backend-Integration-Test, der den gesamten Flow abdeckt:

1. Register OH mit gültigem Keypair
2. Sende eine Nachricht an das OH (via FlaschenpostPut oder direct deposit)
3. Fetch die Nachricht mit gültigem Keypair
4. Verifiziere, dass Payload korrekt ist
5. Revoke das OH

## Protobuf Changes

Keine Änderungen an `outbound.proto`. Die bestehenden Messages sind ausreichend.

## Backend Changes

| File | Action |
|------|--------|
| `InboundCommandProcessor.java` | FlaschenpostPut → OH-Mailbox Routing (wenn `oh_id` lokal bekannt) |
| `OutboundService.java` | Neue Methode `depositMessage(ohId, payload)` — wraps mailbox store |
| **New**: `OutboundServiceIntegrationTest.java` | End-to-End Test: register → deposit → fetch → revoke |

## Acceptance Criteria

- [ ] `RegisterOhRequest` wird korrekt verarbeitet; Response enthält `status == OK` und `expires_at_ms`
- [ ] Eine `FlaschenpostPut` Nachricht an ein registriertes OH wird in der Mailbox abgelegt
- [ ] `FetchRequest` gibt die abgelegte Nachricht zurück
- [ ] `RevokeOhRequest` entfernt das OH und seine Mailbox
- [ ] Integration-Test deckt den gesamten Flow ab
- [ ] Wire-Protokoll (Command Bytes, Signing Bytes) ist dokumentiert für Frontend-Team

## Open Questions

1. Soll `FlaschenpostPut` direkt eine `oh_id` enthalten, oder muss der Server die Zuordnung über `KademliaId` machen?
2. Braucht der `depositMessage`-Pfad Authentifizierung (aktuell kann jeder an jedes OH senden)?
