# Backend MS08: Group Chat

## Status: N/A — keine Backend-Änderungen

> **Frontend-Alignment**: Kein Backend-Blocker für [Frontend MS08](../frontend/ms08_group_chat.md).
> Group Chat ist reine Frontend-Logik (Fan-Out, Key Rotation, Member Management).

## Goal

Kein Backend-Anteil. Der OH-Service ist group-agnostic — er speichert und liefert opake verschlüsselte Payloads aus. Fan-Out (eine Nachricht an N Member senden) passiert komplett im Frontend.

## Warum kein Backend-Anteil?

1. **OH-Mailbox ist per-User, nicht per-Group**: Jedes Gruppenmitglied hat sein eigenes OH. Der Absender sendet N einzelne Nachrichten an N OHs.
2. **Verschlüsselung ist End-to-End**: Der Server sieht nur opake `bytes payload`. Er weiß nicht, ob eine Nachricht zu einem 1:1-Chat oder einer Gruppe gehört.
3. **Key Rotation ist Client-Logik**: Der neue Gruppen-Key wird per-Member verschlüsselt und als reguläre Nachricht über deren OH zugestellt.
4. **Control Messages** (MemberAdded, MemberRemoved, KeyRotation) sind reguläre `ChannelMessage`-Payloads — der Server routet sie wie jede andere Nachricht.

## Abhängigkeiten

Das Frontend MS08 braucht:
- Frontend MS05 (Reverse Garlic) — RGB für Reply-Paths in der Gruppe
- Backend MS04 (Multi-Hop Relay) — Garlic-Routing für Fan-Out
- Backend MS02 (Reliable Mailbox) — zuverlässige Zustellung an jedes Mitglieds-OH
