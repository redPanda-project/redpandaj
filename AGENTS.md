# AGENTS.md

> **Purpose**: This file provides immediate context for AI agents working on `redpandaj`. Read this first to understand build, test, and architectural patterns.

## Project Identity
- **Name**: `redpandaj`
- **Role**: Java full node for the Redpanda decentralized messaging network.
- **Goal**: Privacy-first, metadata-resistant messaging (WhatsApp-like UX, decentralized core).
- **Mobile Client Relationship**: This node handles routing/storage so mobile clients (Flutter) can stay lightweight/offline.

## Tech Stack
- **Language**: Java 21+
- **Build System**: Maven 3.x
- **Key Libraries**:
    - **Protobuf**: For wire format/serialization.
    - **Bouncy Castle**: Cryptography.
    - **JGraphT**: Graph structures.
    - **MapDB**: Embedded persistence.
    - **Lombok**: Boilerplate reduction (requires annotation processing).

## Critical Commands

### Build
```bash
mvn clean package
# Artifact: target/redpanda.jar
```

### Run
```bash
java -jar target/redpanda.jar
# Default Port: 59558
```

### Testing
- **Unit Tests**: `mvn test`
- **E2E/Integration**: `mvn -Pe2e verify`

## Project Structure
- `src/main/java/im/redpanda` - Core source code.
- `src/test/java` - Unit and integration tests.
- `docs/` - Protocol documentation (read this for deep dives on crypto/routing).
- `pom.xml` - Dependencies and build configuration.

## Developer Notes / Gotchas
1.  **Protobuf Generation**: Sources are generated into `target/generated-sources/protobuf`. If symbol resolution fails, try running `mvn generate-sources` or `mvn compile` first.
2.  **Generated Code**: Do not edit files in `target/`.
3.  **Lombok**: Annotations (`@Getter`, `@Setter`, `@Slf4j`) are used extensively. Ensure your context is aware of generated methods.
4.  **Java 21**: We use modern Java features. Ensure compatibility when suggesting code.
5.  **Strict Filtering**: Recent changes enforce strict peer filtering to prevent redundant connections.
