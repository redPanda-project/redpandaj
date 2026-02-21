# Backend MS07: Push Sender (FCM / APNs)

## Status: Missing

> **Frontend-Alignment**: Backend MS07 ist Voraussetzung für [Frontend MS07](../frontend/ms07_push_notifications.md).
> Push-Token-Verarbeitung und FCM/APNs-Integration müssen serverseitig stehen.

## Goal

Der OH-Node sendet eine content-free Push-Notification (wake-up) an den registrierten Client, wenn eine neue Nachricht in der Mailbox eintrifft. Kein Message-Content in der Push-Payload — nur ein Signal zum Aufwachen und Fetchen.

## Prerequisites

- Backend MS03 (Crypto Migration) — Push-Token muss verschlüsselt registriert werden
- Backend MS01 (OH Service) — `depositMessage()` als Trigger

## Current State

| Component | File | Status |
|-----------|------|--------|
| Push in ARC42 | `02_architecture_constraints.adoc` | Documented |
| Push Actor | `03_system_scope_and_context.adoc` | Diagramm vorhanden |
| Privacy-Risiko | `11_risks_and_technical_debt.adoc` | Notiert |
| Backend Push-Code | — | Missing |

## Spec

### 1. Push-Token Registration

**`RegisterOhRequest` Erweiterung:**

```protobuf
message RegisterOhRequest {
  bytes oh_id = 1;
  bytes oh_auth_public_key = 2;
  int64 requested_expires_at = 3;
  int64 timestamp_ms = 4;
  bytes nonce = 5;
  bytes signature = 6;
  bytes encrypted_push_token = 7;   // NEW: verschlüsselt mit OH-Node's X25519 Key
  uint32 push_provider = 8;         // NEW: 0=none, 1=FCM, 2=APNs
}
```

**`OutboundService.handleRegister()` — Erweiterung:**

1. Wenn `encrypted_push_token` vorhanden:
   - Entschlüsseln mit eigenem X25519 Key (AES-256-GCM).
   - Plain-Text Push-Token + Provider in `HandleRecord` speichern.
2. Token wird **nicht** im Signing-Bytes-Format inkludiert (optional, keine Auth-Bindung).

**`HandleRecord` Erweiterung:**
```java
class HandleRecord {
    byte[] ohAuthPublicKey;
    long createdAtMs, expiresAtMs, lastSeenMs;
    byte[] pushToken;         // NEW: decrypted FCM/APNs token
    int pushProvider;         // NEW: 0=none, 1=FCM, 2=APNs
}
```

### 2. Push Trigger

**`OutboundService.depositMessage()` — nach erfolgreichem Speichern:**

```java
void depositMessage(byte[] ohId, MailItem item) {
    mailboxStore.addMessage(ohId, item);

    HandleRecord handle = handleStore.get(ohId);
    if (handle != null && handle.pushToken != null) {
        pushSender.sendWakeUp(handle.pushToken, handle.pushProvider);
    }
}
```

### 3. PushSender Interface

```java
public interface PushSender {
    void sendWakeUp(byte[] token, int provider);
}
```

**`FcmPushSender`:**
- Firebase Admin SDK (`com.google.firebase:firebase-admin`).
- Sendet `data`-only Message: `{"wake": "1"}` (kein `notification`-Block → kein User-sichtbarer Alert von FCM selbst).
- Fire-and-forget (async, kein Blocking).
- Bei `UNREGISTERED` / `INVALID_ARGUMENT` Response: Token aus `HandleRecord` entfernen.

**`ApnsPushSender`:**
- APNs HTTP/2 Client (z.B. `com.eatthepath:pushy`).
- Silent Push: `content-available: 1`, leerer `alert`.
- Fire-and-forget.
- Bei `BadDeviceToken` Response: Token entfernen.

### 4. Rate Limiting

**`PushRateLimiter`:**

```java
class PushRateLimiter {
    // Pro OH: max 1 Push alle 30 Sekunden
    private final Map<String, Long> lastPushTime = new ConcurrentHashMap<>();

    boolean shouldSend(byte[] ohId) {
        String key = Hex.encode(ohId);
        long now = System.currentTimeMillis();
        Long last = lastPushTime.get(key);
        if (last != null && now - last < 30_000) return false;
        lastPushTime.put(key, now);
        return true;
    }
}
```

### 5. Konfiguration

**`push_config.json` (oder Environment-Variablen):**

```json
{
  "fcm": {
    "service_account_path": "/path/to/firebase-service-account.json"
  },
  "apns": {
    "key_path": "/path/to/AuthKey_XXXXXXXXXX.p8",
    "key_id": "XXXXXXXXXX",
    "team_id": "YYYYYYYYYY",
    "bundle_id": "im.redpanda.app",
    "production": false
  },
  "enabled": true
}
```

Wenn `enabled = false` (Default): Kein Push, kein Firebase-Dependency. Erlaubt Betrieb ohne Google/Apple-Anbindung.

## Protobuf Changes

```protobuf
// outbound.proto — RegisterOhRequest Erweiterung:
message RegisterOhRequest {
  bytes oh_id = 1;
  bytes oh_auth_public_key = 2;
  int64 requested_expires_at = 3;
  int64 timestamp_ms = 4;
  bytes nonce = 5;
  bytes signature = 6;
  bytes encrypted_push_token = 7;   // NEW
  uint32 push_provider = 8;         // NEW
}
```

## Backend Changes

| File | Action |
|------|--------|
| **New**: `PushSender.java` | Interface |
| **New**: `FcmPushSender.java` | Firebase Admin SDK Integration |
| **New**: `ApnsPushSender.java` | APNs HTTP/2 Client |
| **New**: `PushRateLimiter.java` | Per-OH Rate Limiting (1/30s) |
| `OutboundHandleStore.java` | `HandleRecord` um `pushToken` + `pushProvider` erweitern |
| `OutboundService.java` | Push-Token bei Register entschlüsseln + speichern; Push bei `depositMessage()` triggern |
| `Server.java` | `PushSender` initialisieren, Config laden |
| `pom.xml` / `build.gradle` | Firebase Admin SDK + APNs Library als Dependencies |
| **New**: `push_config.json` | FCM/APNs Credentials-Konfiguration |

## Acceptance Criteria

- [ ] `RegisterOhRequest` mit `encrypted_push_token` wird korrekt verarbeitet; Token entschlüsselt und gespeichert
- [ ] Nachricht in Mailbox → FCM/APNs Wake-Up Push innerhalb von 2 Sekunden
- [ ] Push-Payload ist content-free: nur `{"wake": "1"}` (FCM) bzw. `content-available: 1` (APNs)
- [ ] Rate Limiting: Max 1 Push pro OH pro 30 Sekunden
- [ ] Ungültige Tokens (UNREGISTERED / BadDeviceToken) werden aus dem HandleRecord entfernt
- [ ] `push_config.enabled = false` → kein Push, keine Firebase-Dependency zur Runtime
- [ ] Push-Sending ist async (fire-and-forget) — blockiert nicht den Mailbox-Deposit

## Open Questions

1. Soll Firebase Admin SDK direkt eingebunden werden, oder ein externer Push-Relay-Service?
2. APNs-Library: `pushy` oder `java-apns`?
3. Soll der Push-Token in den Signing-Bytes der Registration enthalten sein (Auth-Bindung)?
4. Wie mit Token-Refresh umgehen — Neuer `RegisterOhRequest` überschreibt den alten Token?
