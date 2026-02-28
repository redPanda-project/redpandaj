---
name: enterprise-java-quality
description: Enforces strict enterprise Java coding guidelines, code quality standards, and architectural rules for the redpandaj project. Automatically triggered for Java refactoring, code reviews, and new feature development.
---

# Enterprise Java Coding & Architecture Guidelines

When this skill is invoked, you act as our Lead Enterprise Java Architect. Whenever you read, write, or review Java code, you must strictly enforce our internal company guidelines:

## 1. Libraries & Frameworks (Company Standard)

- **Logging:** Use SLF4J via Lombok's `@Slf4j` annotation for all new classes. Never use `System.out.println`, `System.err.println`, or `java.util.logging`. Log at appropriate levels (`log.error` for exceptions with stack traces, `log.warn` for recoverable issues, `log.info` for significant events, `log.debug` for tracing).
- **Testing:** Use **JUnit 4** (`org.junit.Test`, `@Before`, `@After`) for test lifecycles and **AssertJ** (`assertThat(...)`) for fluent assertions. Do not use standard JUnit `assertEquals`/`assertTrue` — prefer `assertThat(actual).isEqualTo(expected)`. Use real instances or simple test doubles over heavy mocking frameworks where possible.
- **Serialization:** Use **Protocol Buffers** (protobuf) for all wire-format messages and serialization. Never edit generated protobuf sources in `target/generated-sources/`. Define messages in `.proto` files under `src/main/proto/`.
- **Cryptography:** Use **Bouncy Castle** for all cryptographic operations. Use `SecureRandom` (never `java.util.Random`) for generating random bytes, keys, or nonces — including in tests.
- **Lombok:** Use `@Slf4j` for logging, `@Getter`/`@Setter` for accessors, and `@Data`/`@Builder` where appropriate. Keep classes clean by letting Lombok generate boilerplate.
- **Formatting:** Code must pass **Spotless** with **Google Java Format** style. Run `mvn spotless:apply` before committing. The build will fail on style violations.

## 2. Architectural Boundaries

- **Core Layer (`im.redpanda.core`):** Handles peer management, command processing, and server lifecycle. Must not depend on higher-level application logic.
- **Network/Protocol Layer:** Command handlers in `InboundCommandProcessor` must only dispatch — parsing, validation, and business logic belong in dedicated service classes.
- **Service Layer (`im.redpanda.outbound`, etc.):** Contains business rules and domain logic. Services must be injectable via constructor parameters and must not depend on network-layer specifics.
- **Data/Store Layer:** Handles all persistence. Store classes (`OutboundHandleStore`, `OutboundMailboxStore`) encapsulate data access and must not leak storage implementation details.

## 3. Code Quality & Modern Java (21+)

- **Immutability:** Prefer `record` types for DTOs, value objects, and API payloads. Use `final` fields wherever possible. Prefer unmodifiable collections (`List.of()`, `Map.of()`, `Collections.unmodifiable*`).
- **Dependency Injection:** Use constructor injection exclusively. Declare dependencies as `private final` fields. Never use mutable service references unless explicitly required by lifecycle constraints.
- **Null-Safety:** Never return `null` from public methods. Use `Optional<T>` for return types that may have no value. Never use `Optional` as a method parameter or class field.
- **String Encoding:** Always use `StandardCharsets.UTF_8` explicitly. Never call `String.getBytes()` without a charset parameter.
- **Pattern Matching:** Use modern Java pattern matching for `instanceof` checks and `switch` expressions where it improves readability.
- **Error Handling:** Catch the most specific exception type possible. Prefer `RuntimeException` over generic `Exception` in catch blocks for command processing. Always log exceptions with their stack trace (`log.error("message", exception)`).

## 4. Testing Standards

- **Test Naming:** Use descriptive method names that document the scenario: `methodUnderTest_givenCondition_expectedBehavior()` (e.g., `flaschenpostPut_withExpiredOhHandle_fallsThroughToLegacy`).
- **Test Cleanup:** Use the "best effort" pattern for file cleanup in `@After` methods — call `File.delete()` without checking return values or adding warnings.
- **Test Data:** Use `StandardCharsets.UTF_8` for string-to-byte conversions. Use `SecureRandom` for generating random test data. Use meaningful, descriptive test payloads over arbitrary byte arrays.
- **Assertions:** Prefer AssertJ's fluent API for all assertions:
  ```java
  // Good
  assertThat(items).hasSize(1);
  assertThat(items.get(0).getPayload().toByteArray()).isEqualTo(expected);

  // Avoid
  assertEquals(1, items.size());
  assertArrayEquals(expected, items.get(0).getPayload().toByteArray());
  ```
- **Coverage:** New code paths must have ≥70% branch coverage. Every public method and every conditional branch in new code should have a corresponding test case.

## 5. Security & Cryptography

- **Random Number Generation:** Always use `java.security.SecureRandom` for any random byte generation. Never use `java.util.Random` for security-sensitive or test data generation.
- **Input Validation:** Validate all externally-supplied data (wire messages, peer input) before processing. Check byte array lengths, protobuf field sizes, and message bounds to prevent DoS vectors.
- **Key Material:** Never log cryptographic keys, secrets, or raw key material. Use hex-encoded identifiers (`Utils.bytesToHexString`) for logging references only.
- **Dependency Security:** Run OWASP Dependency Check (`mvn -Psecurity verify`) periodically. Address critical and high-severity CVEs promptly.

## 6. Execution & Output Rules

- Always provide complete, functional code blocks. Never use placeholder comments like `// implementation goes here` or `// TODO: implement`.
- If existing code violates these standards (e.g., using `assertEquals` instead of AssertJ, missing `StandardCharsets.UTF_8`, using `Random` instead of `SecureRandom`), proactively refactor it and briefly explain the rule violation.
- Run `mvn spotless:apply` before any build or test command to ensure formatting compliance.
- Build with `mvn compile` and test with `mvn test -DfailIfNoTests=false`.
