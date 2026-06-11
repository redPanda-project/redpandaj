# Backend MS03b: Forward Secrecy

## Status: Missing (minimaler Backend-Anteil)

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

Akzeptanzkriterien und Open Questions: siehe Master-Spec.
