package im.redpanda.updater;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * TD130: which apk the signing step picks up. The default used to be a Windows-spelled literal
 * ({@code ..\app\build\app\outputs\apk\release\app-release.apk}), which on Linux is one path
 * segment whose name contains backslashes — {@code lastModified()} returns 0 and the read fails, on
 * the very host that signs the testnet releases.
 */
class UpdaterAndroidSourceTest {

  @AfterEach
  void cleanup() {
    System.clearProperty(Updater.ANDROID_APK_SOURCE_PROPERTY);
  }

  @Test
  void defaultSourceIsAPortableMultiSegmentPath() {
    assertFalse(
        Updater.DEFAULT_ANDROID_APK_SOURCE.contains("\\"),
        "backslashes are literal filename characters outside Windows");
    Path source = Path.of(Updater.DEFAULT_ANDROID_APK_SOURCE);
    assertEquals("app-release.apk", source.getFileName().toString());
    assertTrue(source.getNameCount() > 1, "must resolve as a real directory chain: " + source);
  }

  @Test
  void systemPropertyOverridesTheDefault() {
    System.setProperty(Updater.ANDROID_APK_SOURCE_PROPERTY, "/somewhere/else/app-release.apk");
    assertEquals(Path.of("/somewhere/else/app-release.apk"), Updater.androidApkSource());
  }

  @Test
  void blankPropertyFallsBackToTheDefault() {
    System.setProperty(Updater.ANDROID_APK_SOURCE_PROPERTY, "  ");
    assertEquals(Path.of(Updater.DEFAULT_ANDROID_APK_SOURCE), Updater.androidApkSource());
  }

  /**
   * A set-but-unusable value must stop the signing run, not fall back: the property names the
   * artefact that is about to be signed with the network's update key, so signing something else
   * than the operator asked for is the one outcome worth crashing over.
   */
  @Test
  void unusablePropertyFailsInsteadOfSigningTheDefault() {
    System.setProperty(Updater.ANDROID_APK_SOURCE_PROPERTY, "no\u0000such\u0000path.apk");
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, Updater::androidApkSource);
    assertTrue(
        thrown.getMessage().contains(Updater.ANDROID_APK_SOURCE_PROPERTY),
        "the message must name the property: " + thrown.getMessage());
  }
}
