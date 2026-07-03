# Backend MS06: R-ACK Generation

## Status: Done (2026-07-03, redpandaj [#229](https://github.com/redPanda-project/redpandaj/pull/229))

> **Frontend-Alignment**: Backend MS06 war Voraussetzung für [Frontend MS06](https://github.com/redPanda-project/docs/blob/main/docs/milestones/frontend/ms06_two_layer_ack.md) — jetzt entblockt.
> Der Node mit der finalen Deposit-Entscheidung generiert R-ACKs und sendet sie als Standard-MS04-Onion über die vom Sender gewählten Return-Path-Hops zurück.
> Verbindliche Festlegungen: [Decisions (Backend-MS06)](https://github.com/redPanda-project/docs/blob/main/docs/milestones/ms06_two_layer_ack.md#decisions-backend-ms06-2026-07-03) in der Master-Spec.

## Goal

Der Node, der die finale Deposit-Entscheidung für eine Garlic-Nachricht trifft (i. d. R. der OH-Host), generiert ein R-ACK (Routing Acknowledgment) und sendet es über den mitgelieferten Return-Path zurück an den Absender. Channel-ACK ist rein Frontend-Logik (kein Backend-Anteil).

## Prerequisites

- Backend MS05 (Reverse Garlic Relay) — `CMD_DELIVER_TAGGED` + `MailItem.session_tag`
- Backend MS04 (Multi-Hop Relay) — Relay-Forwarding für den R-ACK Return-Path

## Current State

| Component | File | Status |
|-----------|------|--------|
| Acked Deliver | `FlaschenpostV2.java` → `CMD_DELIVER_ACKED (0x04)` | Done |
| Return-Path-Block | **New**: `ReturnPath.java` | Done — parse/serialize, Hop-Bounds |
| R-ACK-Versand | **New**: `RoutingAckSender.java` | Done — Onion-Bau, Status-Mapping |
| Layer-Dispatch | `GarlicRouter.java` → `handleDeliverAcked` | Done |
| MS02b-Fallback | `OhForwarder.java`, `GMParser.java`, `InboundCommandProcessor.java` | Done — `FlaschenpostPut.return_path` konserviert den Block |
| R-ACK-Payload | `outbound.proto` → `RoutingAck` | Done |

## Spec (umgesetzt)

> **Hinweis:** Die ursprüngliche Spec sah einen von Alice **vorverschlüsselten** Return-Path
> vor, in den der OH-Node nur den R-ACK-Payload „einsetzt". Das ist mit stateless
> GCM-Relays nicht umsetzbar — exakt das Argument aus
> [MS05 Decision 6](https://github.com/redPanda-project/docs/blob/main/docs/milestones/ms05_reverse_garlic.md#decisions-backend-ms05-2026-06-13):
> jede Layer ist GCM-authentifiziert, nachträglich lässt sich keine Payload einfügen.
> Der Return-Path enthält daher **Hop-Deskriptoren**, und der ackende Node baut die
> R-ACK-Onion selbst (wie Bob beim RGB-Reply).

### 1. Acked Deliver: `CMD_DELIVER_ACKED (0x04)`

Neues Layer-Command (`CMD_DELIVER`/`CMD_DELIVER_TAGGED` bleiben byte-identisch):

```
CMD_DELIVER_ACKED (0x04):
[1  cmd]
[20 oh_id]                 // Ziel-Mailbox (KademliaId)
[1  tag_len]               // 0 oder 16 — sonst Drop
[.. session_tag]           // wie CMD_DELIVER_TAGGED; leer = ungetaggter Forward-Send
[return_path]              // Block, siehe unten
[4  payload_len]
[N  payload]
[optionales Padding]
```

Das längenpräfixierte Tag lässt getaggte Replies (RGB-Pfad) und ungetaggte Forward-Sends
dasselbe Command nutzen.

### 2. Return-Path-Block (`ReturnPath.java`)

```
[20 ack_oh_id]             // Absender-OH, in das der R-ACK deponiert wird
[16 ack_session_tag]       // vom Absender pro Nachricht gewählt — Korrelations-Schlüssel
[1  hop_count]             // 0..4
hop_count × [20 kademlia_id][32 encryption_pub]
```

Maximal 245 B. `hop_count = 0`: der deponierende Node stellt den R-ACK direkt zu
(lokaler Deposit ins `ack_oh_id`, sonst MS02b-Forward zum Host). Validierung ist rein
strukturell (Längen, Hop-Bounds) — die Inhalte verantwortet der Sender: ein unerreichbarer
Return-Path bedeutet schlicht, dass kein R-ACK ankommt.

### 3. R-ACK-Payload (`RoutingAck`)

```protobuf
message RoutingAck {
  int64 timestamp_ms = 1;   // Uhr des deponierenden Nodes
  uint32 status = 2;        // 0=stored, 1=mailbox_full, 2=handle_expired, 3=rejected
}
```

**Kein `message_id`-Feld**: die Mailbox-UUID wird server-seitig beim Deposit vergeben und
ist für den Absender bedeutungslos — die Korrelation läuft über den `ack_session_tag`, der
als `MailItem.session_tag` (MS05) am R-ACK-Item hängt. `status` wird immer explizit gesetzt
(proto3-Default 0 = stored wäre sonst mehrdeutig).

### 4. R-ACK-Versand (`RoutingAckSender`)

Der Node mit der **finalen Deposit-Entscheidung** ackt:

| Deposit-Ergebnis | R-ACK |
|------------------|-------|
| `DEPOSITED` | `0` stored |
| `QUOTA_EXCEEDED` | `1` mailbox_full |
| `NOT_FOUND`, kein Forward-Budget mehr | `2` handle_expired |
| `BAD_REQUEST` | `3` rejected |
| `NOT_FOUND`, MS02b-Forward angenommen | kein R-ACK von diesem Node — der Host ackt |

Die R-ACK-Onion ist ein Standard-MS04-Paket: innerste Schicht `CMD_DELIVER_TAGGED`
(`ack_oh_id` + `ack_session_tag` + `RoutingAck`), außen `CMD_FORWARD`-Schichten in
Deskriptor-Reihenfolge; Versand via `GarlicRouter.routeToNextHop` zum ersten Hop.
Fire-and-forget: Bau-/Routing-Fehler werden geloggt und verworfen, keine Retries
(der R-ACK-Timeout + MS02-Re-Send des Absenders decken Verluste ab).

**Keine R-ACKs für R-ACKs** (innerste Schicht ist `CMD_DELIVER_TAGGED`, nie
`CMD_DELIVER_ACKED`) — Ack-Schleifen sind konstruktiv unmöglich; Amplification ist auf
Faktor 1 begrenzt (1 × 2048 B rein → max. 1 × 2048 B raus).

### 5. MS02b-Fallback (`FlaschenpostPut.return_path`, Feld 6)

Ist der finale Garlic-Hop nicht der OH-Host, konserviert der MS02b-Forward den
Return-Path-Block (Muster = MS05 `session_tag`, Feld 5). Der Host-Node ackt nach seiner
Deposit-Entscheidung. Strukturell ungültige Blöcke lehnen den Deposit mit `BAD_REQUEST`
ab (wie ungültige Session-Tags). Erreicht ein Paket das Hop-Limit auf einem Node ohne das
OH, sendet dieser ein `handle_expired`-R-ACK statt den Absender in den Timeout laufen zu
lassen.

### 6. Byte-Budget

Innermost-Overhead `CMD_DELIVER_ACKED` = 26 + tag (0|16) + Return-Path (37 + 52·h).
Mit Tag + 3 Return-Hops: 235 B ⇒ **max. 1554 B Payload bei 3 Forward-Hops**
(MS04 ungetaggt: 1764, MS05 getaggt: 1748). Die R-ACK-Onion selbst ist trivial
(~15 B Proto-Payload).

## Acceptance Criteria

- [x] Node generiert R-ACK nach der finalen Deposit-Entscheidung
- [x] R-ACK traversiert die Return-Path-Hops wie ein normales MS04-Paket
- [x] R-ACK enthält `timestamp_ms` + `status`; Korrelation via `ack_session_tag`
- [x] `mailbox_full` (Quota) → Status `1`
- [x] `handle_expired` (OH unbekannt am Hop-Limit) → Status `2`
- [x] Kein Return-Path (`CMD_DELIVER`/`CMD_DELIVER_TAGGED`) → kein R-ACK, kein Fehler
- [x] MS02b-Fallback konserviert den Return-Path; der Host ackt
- [x] Integrationstest: `RoutingAckRouterTest` (11 Tests — 2-Relay-Traversal, Zero-Hop, Fehler-Status, Negativpfade)

## Open Questions

Beantwortet, siehe [Decisions (Backend-MS06)](https://github.com/redPanda-project/docs/blob/main/docs/milestones/ms06_two_layer_ack.md#decisions-backend-ms06-2026-07-03):

1. ~~Wie fügt der OH-Node den R-ACK-Payload in den Return-Path ein?~~ → Gar nicht — er baut die Onion selbst aus Hop-Deskriptoren (Decision 2, Konsequenz aus MS05 Decision 6).
2. ~~Soll der OH-Node den Return-Path validieren?~~ → Strukturell ja (Längen, Hop-Bounds ≤ 4); inhaltlich nein (Decision 2).
3. ~~Timeout/Retry beim R-ACK-Versand?~~ → Fire-and-forget; Verluste deckt der sender-seitige Timeout ab (Decision 5).
