package im.redpanda.architecture;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAnyPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

/**
 * Pins the bounded-context package map of T118 (DDD review 2026-08-31 §3).
 *
 * <p>Before T118 the node was cut by technical layer ({@code core}, {@code jobs}, {@code store})
 * instead of by domain, with a {@code core → jobs → core} cycle in the middle. Each production
 * class now lives in exactly one context package, and the few directed edges that the context map
 * promises are checked here rather than described in a comment.
 *
 * <p>Only production classes are imported: test classes deliberately live in mirror packages plus a
 * few helper packages ({@code testutil}, {@code docs}, {@code e2e}, {@code architecture}) that are
 * not part of the context map.
 */
class BoundedContextArchitectureTest {

  /**
   * The context map. {@code im.redpanda} holds the composition root ({@code App}) and {@code
   * im.redpanda.core} the published language (SK1: {@code Command}, {@code WireRegistry}) plus the
   * composition-root state ({@code ServerContext}, {@code Server}, {@code LocalSettings}, {@code
   * StateFormat}, {@code NodeIdCodec}) — neither is a bounded context, which is why they are named
   * separately here.
   */
  private static final String[] CONTEXT_PACKAGES = {
    "im.redpanda", // App, composition root
    "im.redpanda.core", // published language SK1 + composition-root state
    "im.redpanda.transport", // N-TRANSPORT
    "im.redpanda.routing", // N-ROUTING
    "im.redpanda.routing.graph", // N-ROUTING internal module (node graph, scores)
    "im.redpanda.mailbox", // N-MAILBOX
    "im.redpanda.dht", // N-DHT
    "im.redpanda.dht.nodeinfo", // N-DHT record schema
    "im.redpanda.identity", // N-IDENTITY
    "im.redpanda.identity.crypt", // N-IDENTITY supporting library
    "im.redpanda.ops", // N-OPS
    "im.redpanda.updater", // N-UPDATER
    "im.redpanda.crypt.legacy", // frozen serialization tombstone, see LegacyNodeId
    "im.redpanda.proto", // generated, java_package of commands.proto
    "im.redpanda.outbound.v1", // generated, java_package of outbound.proto
  };

  /** Contexts that must not know the updater exists. */
  private static final String[] DOMAIN_PACKAGES = {
    "im.redpanda.routing..",
    "im.redpanda.mailbox..",
    "im.redpanda.dht..",
    "im.redpanda.identity..",
    "im.redpanda.ops..",
  };

  private final JavaClasses productionClasses =
      new ClassFileImporter()
          .withImportOption(new ImportOption.DoNotIncludeTests())
          .importPackages("im.redpanda");

  @Test
  void everyProductionClassLivesInAContextPackage() {
    ArchRule rule =
        classes()
            .should()
            .resideInAnyPackage(CONTEXT_PACKAGES)
            .because(
                "T118 repackaged the node by bounded context; a class outside the map means the"
                    + " map or the class is wrong");

    rule.check(productionClasses);
  }

  @Test
  void theJobsPackageStaysDissolved() {
    ArchRule rule =
        noClasses()
            .should()
            .resideInAnyPackage("im.redpanda.jobs..")
            .because(
                "jobs are protocol sagas and belong to their domain; the package existing again"
                    + " would bring back the core <-> jobs cycle the DDD review found");

    rule.check(productionClasses);
  }

  @Test
  void identityIsALeafContext() {
    ArchRule rule =
        noClasses()
            .that()
            .resideInAnyPackage("im.redpanda.identity..")
            .should()
            .dependOnClassesThat(
                resideInAPackage("im.redpanda..")
                    .and(not(resideInAnyPackage("im.redpanda.identity.."))))
            .because(
                "NodeId/KademliaId/crypt are used by every other context, so identity must not"
                    + " depend on any of them");

    rule.check(productionClasses);
  }

  @Test
  void onlyTheCompositionRootAndTransportKnowTheUpdater() {
    ArchRule rule =
        noClasses()
            .that()
            .resideInAnyPackage(DOMAIN_PACKAGES)
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("im.redpanda.updater..")
            .because(
                "the updater is its own bounded context that only shares the command namespace"
                    + " (review §3); it is wired in by Server and the wire dispatcher, nowhere"
                    + " else");

    rule.check(productionClasses);
  }

  @Test
  void theLegacySerializationTombstoneStaysUnreferenced() {
    ArchRule rule =
        noClasses()
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("im.redpanda.crypt.legacy..")
            .because(
                "LegacyNodeId only exists so a pre-T117 object stream can still be consumed; a"
                    + " reference from live code would mean the removed v22 identity is back");

    rule.check(productionClasses);
  }
}
