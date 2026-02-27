# Backend MS09: Reputation & Anti-Sybil

## Status: Missing

> **Frontend-Alignment**: Backend MS09 ist Voraussetzung für [Frontend MS09](../frontend/ms09_incentive_system.md).
> Der ReputationService und die DHT-basierte Report-Speicherung müssen stehen.

## Goal

Serverseitiges Reputation-System: Reputation-Reports in der DHT speichern und abfragbar machen. Graduated PoW-Difficulty für Anti-Sybil. Nodes können die Reputation anderer Nodes abfragen, um Routing-Entscheidungen zu verbessern.

## Prerequisites

- Backend MS06 (R-ACK Generation) — R-ACK-Daten sind Input für Reputation-Scoring

## Current State

| Component | File | Status |
|-----------|------|--------|
| HashCash PoW | `NodeId.java` — `SHA256(SHA256(pubkey))[0] == 0` | Done — niedrige Difficulty (8 Bit) |
| Peer Performance | `PeerPerformanceTestSchedulerJob.java` | Done — periodische Health-Checks |
| Peer Stats | `redpanda_light_client.dart` — Latency, success/failure | Done (nur lokal) |
| Reputation System | — | Missing |

## Spec

### 1. Graduated PoW

**`NodeId.java` — PoW-Difficulty-Tiers:**

```java
int getPoWDifficulty() {
    byte[] hash = SHA256(SHA256(verifyKey));
    return countLeadingZeroBits(hash);
}
```

| Tier | Leading Zero Bits | Approx. Generation | Trust Bonus |
|------|-------------------|-------------------|-------------|
| 0 | 8 (aktuell) | Millisekunden | +0.00 |
| 1 | 16 | Sekunden | +0.05 |
| 2 | 20 | Minuten | +0.10 |
| 3 | 24 | Stunden | +0.15 |

- PoW-Tier ist von jedem Node verifizierbar.
- Bestehende NodeIds (8-bit) bleiben gültig (Tier 0).
- Höhere Difficulty = höherer Initial-Score = bevorzugt als Relay-Hop.

### 2. ReputationReport

```protobuf
message ReputationReport {
  bytes reporter_id = 1;        // 20-byte KademliaId des Reporters
  bytes subject_id = 2;         // 20-byte KademliaId des bewerteten Nodes
  uint32 observation_type = 3;  // Enum: RELAY_SUCCESS, RELAY_FAILURE, OH_UPTIME, ...
  int64 timestamp = 4;
  bytes signature = 5;          // Ed25519 Signatur des Reporters
  bytes data = 6;               // observation-spezifische Daten (optional)
}
```

**Observation Types:**

| Type | Value | Description |
|------|-------|-------------|
| RELAY_SUCCESS | 1 | R-ACK für eine Nachricht über diesen Relay erhalten |
| RELAY_FAILURE | 2 | Kein R-ACK für eine Nachricht über diesen Relay |
| OH_UPTIME | 3 | OH-Node war erreichbar (periodischer Check) |
| OH_DOWNTIME | 4 | OH-Node war nicht erreichbar |

### 3. ReputationStore

**DHT-basierte Speicherung:**

Reports werden als `KadContent` in der Kademlia-DHT gespeichert:
- Key: `SHA256("reputation" + subject_id + date_string)[0..20]` — rotiert täglich.
- Content: Serialisierter `ReputationReport`.
- Signatur: Ed25519 des Reporters.

**`ReputationStore.java`:**
```java
class ReputationStore {
    void storeReport(ReputationReport report);            // → KademliaInsertJob
    List<ReputationReport> getReports(KademliaId subjectId); // → KademliaGetJob
}
```

### 4. ReputationService

**Score-Berechnung (Query von Clients oder anderen Nodes):**

```java
class ReputationService {
    NodeReputation getReputation(KademliaId nodeId) {
        List<ReputationReport> reports = reputationStore.getReports(nodeId);

        // Self-Reports ignorieren
        reports = reports.stream()
            .filter(r -> !r.reporterId.equals(nodeId))
            .toList();

        // Score berechnen
        float relayRate = calcRelaySuccessRate(reports);
        float uptimeScore = calcUptimeScore(reports);
        int powDifficulty = getPoWDifficulty(nodeId);
        int ageDays = getNodeAge(nodeId);

        float score = 0.35f * relayRate
                    + 0.20f * normalize(uptimeScore)
                    + 0.15f * normalize(ageDays)
                    + 0.15f * normalize(powDifficulty)
                    + 0.15f * uptimeScore;

        return new NodeReputation(nodeId, score, reports.size());
    }
}
```

### 5. Reputation Query Command

Neuer Command, damit Clients die Reputation eines Nodes abfragen können:

```
Client → Server:
  [1 CMD_REPUTATION_QUERY] [4 length] [NodeReputationQuery protobuf]

Server → Client:
  [1 CMD_REPUTATION_QUERY] [4 length] [NodeReputationResponse protobuf]
```

### 6. Missbrauchs-Schutz

- **Self-Reports**: Reporter == Subject → ignoriert.
- **Reporter-Gewichtung**: Reports von höher-reputablen Reportern zählen mehr (transitive Trust).
- **Rate Limiting**: Max 10 Reports pro Reporter pro Stunde pro Subject.
- **Report-Expiry**: Reports älter als 30 Tage werden bei der Score-Berechnung ignoriert.

## Protobuf Changes

```protobuf
// Neues Proto (reputation.proto oder in commands.proto):

message ReputationReport {
  bytes reporter_id = 1;
  bytes subject_id = 2;
  uint32 observation_type = 3;
  int64 timestamp = 4;
  bytes signature = 5;
  bytes data = 6;
}

message NodeReputationQuery {
  bytes node_id = 1;
}

message NodeReputationResponse {
  bytes node_id = 1;
  float composite_score = 2;
  uint32 report_count = 3;
  uint32 pow_difficulty = 4;
  repeated ReputationReport recent_reports = 5;
}
```

## Backend Changes

| File | Action |
|------|--------|
| `NodeId.java` | `getPoWDifficulty()` — leading zero bits zählen; graduated Tiers |
| **New**: `ReputationReport.java` | Report Data Model |
| **New**: `ReputationStore.java` | DHT-basierte Speicherung/Abfrage von Reports |
| **New**: `ReputationService.java` | Score-Berechnung, Report-Aggregation, Missbrauchs-Filter |
| `InboundCommandProcessor.java` | `CMD_REPUTATION_QUERY` + `CMD_REPUTATION_REPORT` Handler |
| `KadStoreManager.java` | Reputation-Reports als KadContent-Typ unterstützen |
| `PeerPerformanceTestSchedulerJob.java` | Ergebnisse als ReputationReports in die DHT schreiben |

## Acceptance Criteria

- [ ] `NodeId.getPoWDifficulty()` gibt korrekte Tier-Stufe zurück (8/16/20/24 leading zero bits)
- [ ] ReputationReports werden in der DHT gespeichert und sind über `KademliaGet` abrufbar
- [ ] Self-Reports werden beim Score-Berechnung ignoriert
- [ ] `CMD_REPUTATION_QUERY` gibt composite_score + pow_difficulty + report_count zurück
- [ ] Reports älter als 30 Tage werden bei der Score-Berechnung ignoriert
- [ ] Rate Limiting: Max 10 Reports pro Reporter/Subject/Stunde
- [ ] PoW Tier 0 (8 Bit) Nodes funktionieren weiterhin (backward compatible)
- [ ] Reputation-Daten persistieren über Node-Restarts (DHT)

## Open Questions

1. Soll Reputation global (aus DHT aggregiert) oder lokal (nur eigene Beobachtungen) berechnet werden?
2. Wie verhindert man Reputation-Poisoning durch kooperierende Sybil-Nodes?
3. Braucht es einen initialen Seed von vertrauenswürdigen Reportern (Trust Anchors)?
4. Soll monetäre Incentivierung (Crypto-Token) perspektivisch ergänzt werden?
5. Wie viel DHT-Storage verbrauchen Reputation-Reports bei Netzwerkgröße >10k Nodes?
