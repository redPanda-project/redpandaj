# Backend MS02: Reliable Mailbox & Lifecycle

## Status: Partial

> **Frontend-Alignment**: Backend MS02 ist Voraussetzung für [Frontend MS02](../frontend/ms02_reliable_delivery.md).
> Das Frontend braucht das sequence-basierte Fetch + AckFetch-Command.

## Goal

Mailbox von index-basiertem PoC auf sequence-basierte Queue umstellen. Delete-after-acknowledge einführen. OH-Lifecycle (Expiry, Cleanup) robuster machen.

## Prerequisites

- Backend MS01 (OH Service Stabilization) — FlaschenpostPut → OH-Mailbox funktioniert

## Current State

| Component | File | Status |
|-----------|------|--------|
| Mailbox store | `OutboundMailboxStore.java` | `ArrayList<byte[]>`, cursor = list index, no delete |
| Handle lifecycle | `OutboundHandleStore.java` | TTL clamp 10min–7d, `cleanupExpired()` exists |
| Fetch pagination | `outbound.proto` → `FetchRequest.cursor` | cursor = list index |

## Spec

### 1. Sequence-Based Mailbox

**`OutboundMailboxStore.java` — Umbau:**

- Jedes `MailItem` bekommt eine monoton steigende `sequence_id` (long, pro OH).
- Storage: MapDB `BTreeMap<Long, byte[]>` pro OH (oder composite key `ohId_hex + "_" + seqId`).
- `addMessage(ohId, mailItem)`: nächste `sequence_id` vergeben, speichern, committen.
- `fetchMessages(ohId, afterSequence, limit)`: Items mit `sequence_id > afterSequence`, aufsteigend sortiert, max `limit`.
- `deleteUpTo(ohId, sequenceId)`: Alle Items mit `sequence_id <= sequenceId` löschen, committen.

### 2. AckFetch Command

Neuer Command `CMD_ACK_FETCH`:

```
Client → Server:
  [1 CMD_ACK_FETCH] [4 length] [AckFetchRequest protobuf]

Server → Client:
  [1 CMD_ACK_FETCH] [4 length] [AckFetchResponse protobuf]
```

**`OutboundService.handleAckFetch()`:**
1. Signatur verifizieren (gleicher Flow wie Fetch).
2. `OutboundMailboxStore.deleteUpTo(ohId, acked_sequence_id)`.
3. Response mit `status == OK`.

### 3. OH-Expiry Cleanup Job

**`Server.startUpRoutines()`:**
- Neuer periodischer Job (alle 10 Minuten): `OutboundHandleStore.cleanupExpired(now)`.
- Beim Cleanup eines expired Handle auch `OutboundMailboxStore.deleteAll(ohId)`.

### 4. Mailbox Overflow Signaling

- `MAX_ITEMS_PER_MAILBOX = 500` bleibt.
- `addMessage()`: Wenn Mailbox voll, FIFO-Eviction (älteste raus) — besteht bereits.
- `FetchResponse` bekommt ein `mailbox_overflow` Flag, das gesetzt wird, wenn seit dem letzten Fetch Items evicted wurden.

### 5. FetchResponse Anpassung

`next_cursor` in `FetchResponse` ist jetzt die höchste `sequence_id` der zurückgegebenen Items (nicht mehr ein List-Index).

## Protobuf Changes

**`outbound.proto`:**

```protobuf
message MailItem {
  bytes message_id = 1;
  int64 received_at_ms = 2;
  bytes payload = 3;
  uint64 sequence_id = 4;        // NEW
}

message FetchResponse {
  Status status = 1;
  uint64 next_cursor = 2;        // = highest sequence_id returned
  repeated MailItem items = 3;
  int64 server_time_ms = 4;
  bool mailbox_overflow = 5;     // NEW
}

// NEW:
message AckFetchRequest {
  bytes oh_id = 1;
  uint64 acked_sequence_id = 2;
  int64 timestamp_ms = 3;
  bytes nonce = 4;
  bytes signature = 5;
}

message AckFetchResponse {
  Status status = 1;
  int64 server_time_ms = 2;
}
```

## Backend Changes

| File | Action |
|------|--------|
| `OutboundMailboxStore.java` | `ArrayList<byte[]>` → `BTreeMap<Long, byte[]>` mit sequence counter; `deleteUpTo()` implementieren |
| `OutboundService.java` | `handleAckFetch()` hinzufügen |
| `OutboundHandleStore.java` | `cleanupExpired()` erweitern: auch Mailbox löschen |
| `InboundCommandProcessor.java` | `CMD_ACK_FETCH` registrieren, an `OutboundService` delegieren |
| `Server.java` | Periodischen Cleanup-Job registrieren (alle 10 Min) |
| **New**: `OutboundMailboxStoreTest.java` | Unit-Tests für sequence-basierte Mailbox, deleteUpTo, overflow |

## Acceptance Criteria

- [ ] `MailItem` hat eine monotone `sequence_id` pro OH
- [ ] `FetchResponse.next_cursor` ist die höchste `sequence_id` (nicht List-Index)
- [ ] `AckFetchRequest` löscht alle Items mit `sequence_id <= acked_sequence_id`
- [ ] Expired OHs werden alle 10 Minuten gecleant inkl. ihrer Mailboxen
- [ ] Mailbox-Overflow (>500 Items) setzt `mailbox_overflow = true` in der nächsten `FetchResponse`
- [ ] Signing-Bytes für `AckFetch` sind dokumentiert: `[CMD_BYTE | oh_id | acked_sequence_id(8) | timestamp(8) | nonce]`

## Open Questions

1. Soll `AckFetch` ein eigener Command sein oder als Flag auf `FetchRequest` piggybacked?
2. Soll der Overflow-Counter pro OH persistent sein, oder reicht ein transientes Flag?
