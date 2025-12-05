package im.redpanda.architecture;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaCall;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.Assert;
import org.junit.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

public class AssertionsArchitectureTest {

    private final JavaClasses classes = new ClassFileImporter().importPackages("im.redpanda");

    @Test
    public void hamcrestShouldNotBeUsed() {
        ArchRule rule = noClasses()
                .that().resideInAnyPackage("im.redpanda..")
                .should().dependOnClassesThat().resideInAnyPackage("org.hamcrest..")
                .because("AssertJ assertThat should be used instead of Hamcrest matchers");
        rule.check(classes);
    }

    @Test
    public void junitAssertThatShouldNotBeUsed() {
        ArchRule rule = noClasses()
                .that().resideInAnyPackage("im.redpanda..")
                .should().callMethodWhere(isJUnitAssertThat())
                .because("AssertJ assertThat should be used instead of JUnit/Hamcrest assertThat");
        rule.check(classes);
    }

    private DescribedPredicate<JavaCall<?>> isJUnitAssertThat() {
        return new DescribedPredicate<>("call to org.junit.Assert.assertThat") {
            @Override
            public boolean test(JavaCall<?> input) {
                return input.getTarget().getOwner().isEquivalentTo(Assert.class)
                        && input.getTarget().getName().equals("assertThat");
            }
        };
    }
}
