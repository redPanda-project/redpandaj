# Backend MS03b: Forward Secrecy

## Status: Done (2026-06-12 — Verifikation, keine Code-Änderung)

> **Master-Spec**: [Master-Spec im docs-Repo](https://github.com/redPanda-project/docs/blob/main/docs/milestones/ms03b_forward_secrecy.md) — der Ratchet
> läuft Ende-zu-Ende zwischen den Clients; Relays und OH-Mailboxen transportieren weiterhin
> opake Payloads.
> **Frontend-Alignment**: [Frontend MS03b](https://github.com/redPanda-project/docs/blob/main/docs/milestones/frontend/ms03b_forward_secrecy.md) (Hauptanteil).

## Scope (Backend)

1. **Stage 1 (symmetrischer HKDF-Ratchet): voraussichtlich keine Backend-Änderung.**
   Die Payload bleibt für den Server ein opaker Byte-Block; Format-Änderungen finden
   innerhalb des verschlüsselten Containers statt.
2. **Verifikation statt Implementierung**: Sicherstellen, dass kein Backend-Codepfad Annahmen
   über das innere Payload-Format trifft (Deposit, Mailbox, Forwarding behandeln `payload`
   als Bytes — Stand heute erfüllt).
3. **Größenbudget prüfen**: Ratchet-Header (Counter, ggf. ephemere Keys in Stage 2) vergrößern
   die Payload — muss innerhalb der Flaschenpost-Limits bleiben (relevant für MS04-Padding,
   2048-Byte-Pakete).

## Verifikationsergebnis (2026-06-12)

Wie erwartet **keine Code-Änderung nötig** (deshalb kein Backend-PR):

1. **Deposit**: `OutboundService.depositMessage(byte[] ohId, byte[] payload)` übernimmt die
   Payload unverändert als `ByteString` in das `MailItem` — kein Parsen des Inhalts.
2. **Mailbox**: `OutboundMailboxStore` speichert/liefert serialisierte `MailItem`s; einziges
   inhaltsunabhängiges Limit ist `MAX_ITEM_BYTES = 64 KiB` pro serialisiertem Item
   (plus `MAX_ITEMS_PER_MAILBOX = 500` und Byte-Quota, MS02b).
3. **Forwarding**: `OhForwarder`/`GMParser.sendFpToPeer` reichen `byte[] content` opak weiter
   (oh_id + hop_count, max. 3 Hops) — keine Annahme über das Payload-Format.
4. **Größenbudget**: Envelope v4 hat **69 Bytes Festoverhead** (v3: 29; Δ +40 durch
   `ratchet_pub` 32 + `prev_chain_len` 4 + `chain_counter` 4). Gegen das 64-KiB-Limit
   irrelevant; für MS04 (2048-Byte-Padding) verbleiben ~1,9 KiB für Content +
   FlaschenpostPut-Wrapper — Budget dokumentiert in Master-Spec Decision 7.

Die E2E-Suite von redpanda-mobile (Alice→Bob über das Referenz-JAR) läuft mit v4-Payloads
unverändert grün — bestätigt die Opazität in der Praxis.

Akzeptanzkriterien und Decisions: siehe Master-Spec.
