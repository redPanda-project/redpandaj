package im.redpanda.arch;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

/**
 * Guards the JUnit 5 migration (T95): no production or test class may depend on JUnit 4 APIs.
 *
 * <p>Note: {@code resideInAPackage("org.junit")} matches the top-level package only (no
 * subpackages), which is intentional — {@code org.junit.jupiter..} and {@code org.junit.platform..}
 * must remain allowed.
 */
public class JUnit4BanTest {

  @Test
  public void noJUnit4Dependencies() {
    // Imports from the classpath (covers both target/classes and target/test-classes when run
    // via surefire) and stays robust for IDE / non-Maven runners.
    JavaClasses classes = new ClassFileImporter().importPackages("im.redpanda");

    ArchRule rule =
        noClasses()
            .that()
            .resideInAPackage("im.redpanda..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage(
                "org.junit",
                "org.junit.rules..",
                "org.junit.runner..",
                "org.junit.runners..",
                "org.junit.experimental..",
                "junit.framework..")
            .because("T95 migrated all tests to JUnit 5; JUnit 4 APIs are banned");

    rule.check(classes);
  }
}
