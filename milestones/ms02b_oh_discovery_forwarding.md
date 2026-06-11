# Backend MS02b: OH Discovery & Forwarding

## Status: Missing

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
