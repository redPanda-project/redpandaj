package im.redpanda.updater;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
}
