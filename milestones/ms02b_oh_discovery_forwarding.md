# Backend MS02b: OH Discovery & Forwarding

## Status: Done (2026-06-11)

Umgesetzt in redpandaj [#217](https://github.com/redPanda-project/redpandaj/pull/217) (Deposit-Härtung), [#218](https://github.com/redPanda-project/redpandaj/pull/218) (OH→Node-Discovery) und [#219](https://github.com/redPanda-project/redpandaj/pull/219) (Forwarding Option A). Entscheidungen und beantwortete Open Questions: siehe [Decisions in der Master-Spec](https://github.com/redPanda-project/docs/blob/main/docs/milestones/ms02b_oh_discovery_forwarding.md#decisions-backend-2026-06-11).

**Implementiert:**
- **Forwarding (Option A):** `sendFpToPeer()` trägt `oh_id` + `hop_count` (max. 3); nicht-lokale Deposits werden via DHT-Record zum Host-Node geroutet (`OhForwarder`); expliziter `oh_id`-Pfad ist autoritativ (kein Legacy-Fallthrough mehr).
- **Discovery:** Announce-Keypair deterministisch aus oh_id abgeleitet (`OhDht`, Domain-Tag `redpanda.oh.announce.v1`), `OhNodeRecord` fix 256 Bytes gepadded, nur Node-Id im Record; `OhAnnounceJob` (30 ± 5 min + Stagger, Announce direkt nach Register), `OhResolveJob` (Lookup mit 0–1,5 s Zufallsdelay).
- **Deposit-Härtung:** reject-new statt drop-oldest, 64 KiB Per-Item-Limit, 4 MiB Byte-Quota pro Mailbox, Register-Rate-Limit 5/min pro Verbindung; Status-Codes `RATE_LIMIT`/`QUOTA_EXCEEDED`/`BAD_REQUEST` erstmals verdrahtet via Opt-in-Response (`want_response`, Command 158, nur Light Clients).
- **Domänentrennung:** Legacy-Garlic-Header-Fallback (`tryDepositToLocalOh`) als *scheduled for removal* dokumentiert.

> **Master-Spec**: [Master-Spec im docs-Repo](https://github.com/redPanda-project/docs/blob/main/docs/milestones/ms02b_oh_discovery_forwarding.md) — MS02b ist fast vollständig Backend-Arbeit; dies ist die Backend-Sicht.
> **Frontend-Alignment**: [Frontend MS02b](https://github.com/redPanda-project/docs/blob/main/docs/milestones/frontend/ms02b_oh_discovery_forwarding.md) (kleiner Anteil).

## Warum

Heute funktioniert OH-Zustellung nur, wenn der Sender direkt mit dem OH-Host des Empfängers
verbunden ist: `GMParser.sendFpToPeer()` baut die weitergeleitete `FlaschenpostPut` ohne das
`oh_id`-Feld neu auf, und es gibt keine OH→Node-Auflösung. Außerdem ist `depositMessage()`
unauthentifiziert und die Mailbox evictet drop-oldest — ein Angreifer mit bekannter `oh_id`
kann echte Nachrichten still verdrängen (Mailbox-Flush).

## Scope (Backend)

1. **Forwarding-Entscheidung** (Option A/B, siehe Master-Spec): `oh_id` beim Forwarding
   erhalten (`sendFpToPeer()` um `oh_id`-Parameter erweitern) **oder** Zustellung
   ausschließlich über die Garlic-Destination und den expliziten `oh_id`-Pfad auf direkte
   Light-Client-Verbindungen beschränken und dokumentieren.
2. **OH→Node-Auflösung**: DHT-Announce `H(oh_id) → NodeInfo` mit Padding/Delay gegen
   Lookup-Profiling (der QR-Endpoint im Channel bleibt der kurzfristige Pfad).
3. **Deposit-Härtung**: reject-new-Eviction bei voller Mailbox (statt drop-oldest),
   Größenlimit pro `MailItem`, Byte-Quota pro Mailbox, Register-Rate-Limit pro Verbindung.
   Die Proto-Status-Codes `RATE_LIMIT` / `QUOTA_EXCEEDED` / `BAD_REQUEST` existieren bereits
   und werden hier erstmals gesendet.
4. **`oh_id`-Domänentrennung** gegenüber Node-KademliaIds (z. B. `H(domain_tag || pubkey)`)
   oder geplante Abkündigung des Legacy-Garlic-Header-Fallbacks.

## Betroffene Dateien (erwartet)

| Datei | Änderung |
|-------|----------|
| `flaschenpost/GMParser.java` | `oh_id` beim Forwarding erhalten bzw. Pfad-Einschränkung |
| `outbound/OutboundService.java` | Eviction-Strategie, Quotas, Rate-Limit, Status-Codes |
| `outbound/OutboundMailboxStore.java` | reject-new statt drop-oldest, Byte-Zählung |
| `kademlia/…` | OH-Announce/Lookup mit Anti-Profiling-Maßnahmen |

Akzeptanzkriterien und Open Questions: siehe Master-Spec.
