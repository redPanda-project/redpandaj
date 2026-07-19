package im.redpanda.crypt.legacy;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serial;
import java.io.Serializable;

/**
 * Serialization tombstone: instances of the removed pre-MS03 brainpool identity live inside every
 * {@code localSettings*.dat} file written before 2026-07 (the {@code LocalSettings.legacyIdentity}
 * field of that era). Deleting this class — or changing its pinned {@code serialVersionUID} — makes
 * those files unreadable, and {@code LocalSettings.load()} then silently falls back to FRESH
 * settings: every deployed node would lose its identity on the next restart. Never delete while any
 * deployed node may still hold such a file. Guarded by {@code LocalSettingsLegacyFixtureTest}.
 *
 * <p>The full v22 implementation (brainpool ECDH/ECDSA, AES-CTR handshake support) was removed with
 * protocol-v22 support (sdd02 phase 2); this stub only consumes the serialized bytes and discards
 * them.
 */
@Deprecated
public final class LegacyNodeId implements Serializable {

  /** Pinned to the default-computed UID of the removed full implementation. */
  @Serial private static final long serialVersionUID = -259634379899283099L;

  private static final int PUBLIC_KEYLEN = 65;
  private static final int PRIVATE_KEYLEN = 252;

  /**
   * Consumes the custom wire format of the removed implementation ({@code writeObject} wrote a
   * boolean plus the raw key export, no default field data) and discards it.
   */
  @Serial
  private void readObject(ObjectInputStream aInputStream) throws IOException {
    boolean hasPrivate = aInputStream.readBoolean();
    aInputStream.readFully(new byte[hasPrivate ? PRIVATE_KEYLEN : PUBLIC_KEYLEN]);
  }
}
