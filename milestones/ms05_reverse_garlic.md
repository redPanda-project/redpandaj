# Backend MS05: Reverse Garlic (Relay-Seite)

## Status: Done (2026-06-13, redpandaj [#226](https://github.com/redPanda-project/redpandaj/pull/226))

> **Frontend-Alignment**: Backend MS05 ist Voraussetzung für [Frontend MS05](https://github.com/redPanda-project/docs/blob/main/docs/milestones/frontend/ms05_reverse_garlic.md).
> Der Server legt getaggte Delivers (`CMD_DELIVER_TAGGED`) mit `session_tag` in der Mailbox ab.
> Verbindliche Festlegungen: [Decisions (Backend-MS05)](https://github.com/redPanda-project/docs/blob/main/docs/milestones/ms05_reverse_garlic.md#decisions-backend-ms05-2026-06-13) in der Master-Spec.

## Goal

Aus Backend-Sicht ist Reverse Garlic identisch mit Forward Garlic — die Relays peelen Layers und leiten weiter (MS04-Logik). Der einzige Unterschied: Bei einem getaggten Deliver muss der `session_tag` mit in die Mailbox geschrieben werden, damit das Frontend eingehende Replies dem richtigen Channel zuordnen kann.

## Prerequisites

- Backend MS04 (Multi-Hop Relay) — Layer-Peeling funktioniert

## Current State

| Component | File | Status |
|-----------|------|--------|
| Relay-Peeling | `GarlicRouter.java` (aus MS04) | Done — unverändert für FORWARD; neuer `CMD_DELIVER_TAGGED`-Zweig |
| OH Mailbox | `OutboundMailboxStore.java` | Done — `MailItem.session_tag` wird mitserialisiert |
| Deposit-API | `OutboundService.java` | Done — `depositMessage(ohId, payload, sessionTag)` |
| MS02b-Fallback | `OhForwarder.java`, `GMParser.java` | Done — `FlaschenpostPut.session_tag` konserviert den Tag |

## Spec (umgesetzt)

### 1. Getaggter Deliver: `CMD_DELIVER_TAGGED (0x03)`

Statt einer In-Place-Änderung von `CMD_DELIVER` (hätte die released Frontend-MS04-Clients
gebrochen) gibt es ein **neues Layer-Command** — das Layer-Format ist über das Command-Byte
erweiterbar (MS04):

```
CMD_DELIVER        (0x02): [1 cmd][20 oh_id][4 payload_len][payload][opt. Padding]   (unverändert, MS04)
CMD_DELIVER_TAGGED (0x03): [1 cmd][20 oh_id][16 session_tag][4 payload_len][payload][opt. Padding]
```

- `oh_id` = **20 Bytes** (KademliaId — die 32 im früheren Pseudo-Code waren derselbe bekannte
  Fehler wie in MS04, siehe MS04 Decision 4); `payload_len` explizit wie in MS04.
- `session_tag` = 16 Bytes, vom ursprünglichen Sender (Alice) im RGB festgelegt.
- Der Relay-Node sieht den `session_tag` nicht (er ist verschlüsselt in der innersten Layer).
- Nur der finale Hop sieht `oh_id` + `session_tag` + `payload`.

### 2. MailItem-Erweiterung

```protobuf
message MailItem {
  bytes message_id = 1;
  int64 received_at_ms = 2;
  bytes payload = 3;
  uint64 sequence_id = 4;
  bytes session_tag = 5;    // NEW (MS05): 16 bytes, leer für direkte/ungetaggte Nachrichten
}
```

`FetchResponse` liefert den Tag unverändert an den Client. Deposit-Validierung: Tag leer
oder exakt 16 Bytes, sonst `BAD_REQUEST`.

### 3. GarlicRouter

`CMD_DELIVER_TAGGED` wird vom gemeinsamen Deliver-Handler geparst (oh_id, session_tag,
payload_len, payload) und via `outboundService.depositMessage(ohId, payload, sessionTag)`
abgelegt. Bei `NOT_FOUND` greift der MS02b-`OhForwarder`-Fallback — dafür trägt
`FlaschenpostPut` neu das Feld `session_tag = 5`, sodass der Tag den Forward zum
OH-Host-Node überlebt.

### 4. Relay-Verhalten (keine Änderung)

Relays auf dem Reverse-Garlic-Pfad verhalten sich exakt wie auf dem Forward-Pfad:
- Layer peelen (X25519 + AES-256-GCM, AAD = next_hop)
- `CMD_FORWARD` → next_hop weiterleiten (Rebuild mit frischer packet_id + Padding)
- `CMD_DELIVER`/`CMD_DELIVER_TAGGED` → OH-Mailbox einliefern

Es gibt keine spezielle "Reverse"-Logik auf Relay-Ebene. Die Pfad-Konstruktion passiert
komplett im Frontend (RGB Builder).

## Protobuf Changes

```protobuf
// outbound.proto:
message MailItem {
  // ...
  bytes session_tag = 5;    // NEW (MS05)
}

// commands.proto:
message FlaschenpostPut {
  // ...
  bytes session_tag = 5;    // NEW (MS05): Tag-Erhalt beim MS02b-Fallback-Forwarding
}
```

## Backend Changes

| File | Action |
|------|--------|
| `FlaschenpostV2.java` | Konstanten `CMD_DELIVER_TAGGED (0x03)`, `SESSION_TAG_LEN (16)` |
| `GarlicRouter.java` | Gemeinsamer Deliver-Handler parst optionalen 16-Byte-Tag; Fallback reicht Tag durch |
| `OutboundService.java` | `depositMessage(ohId, payload, sessionTag)` + Längen-Validierung |
| `OhForwarder.java` / `GMParser.java` | `sessionTag`-Passthrough auf dem MS02b-Forward |
| `InboundCommandProcessor.java` | `FlaschenpostPut.session_tag` validieren + durchreichen |
| `outbound.proto` / `commands.proto` | `session_tag`-Felder (siehe oben) |

## Acceptance Criteria

- [x] Getaggter Deliver-Plaintext mit `[oh_id][session_tag][payload_len][payload]` wird korrekt geparst *(`CMD_DELIVER_TAGGED`, `ReverseGarlicRouterTest`)*
- [x] `MailItem` in der Mailbox enthält den `session_tag` (16 Bytes)
- [x] `FetchResponse` liefert `MailItem` inkl. `session_tag` an den Client *(`OutboundServiceIntegrationTest`)*
- [x] Nachrichten ohne `session_tag` (direct messages, `CMD_DELIVER`) funktionieren weiterhin (leeres Feld)
- [x] Relays auf dem Reverse-Pfad verhalten sich identisch zu Forward-Relays *(3-Relay-E2E `ReverseGarlicRouterTest`; Negativtests: Replay-Dedup, verkürzte Tagged-Layer, ungültige payload_len, Remote-OH-Fallback mit Tag-Erhalt)*

## Open Questions

Beantwortet, siehe [Decisions (Backend-MS05)](https://github.com/redPanda-project/docs/blob/main/docs/milestones/ms05_reverse_garlic.md#decisions-backend-ms05-2026-06-13):

1. ~~Soll `session_tag` ein required oder optional Feld sein?~~ → Optional (leer für direkte Nachrichten), rückwärtskompatibel (Decision 2).
2. ~~Braucht der OH-Node Kenntnis über die Herkunft (Forward vs. Reverse)?~~ → Nein — die Tag-Präsenz ist die einzige nötige Unterscheidung (Decision 4).
