# AetherFlow

AetherFlow is a high-performance binary protocol library for distributed systems communication. It provides a comprehensive suite of features including connection pooling, state management, packet reassembly, compression, encryption, and sophisticated reliability mechanisms.

## Features

- **Binary Protocol**: Efficient binary wire格式 with support for multiple message types
- **Connection Management**: Sophisticated connection pool with LRU eviction, TTL management, and health checks
- **State Machine**: 20+ state connection state machine with complex transition logic
- **Packet Reassembly**: Fragmentation, reordering, and deduplication for reliable data transfer
- **Session Management**: Authentication, authorization, and session lifecycle management
- **Compression**: Multiple codec support (GZIP, Deflate, LZ4, ZSTD, Snappy)
- **Encryption**: AES-GCM encryption with key rotation and session key management
- **Retry Logic**: Exponential backoff with circuit breakers for resilient operations
- **Metrics**: Comprehensive metrics collection and observability
- **Async Processing**: Thread-safe async message processing with backpressure handling

## Architecture

AetherFlow is organized into the following modules:

- `ProtocolMessage`: Core message structure and serialization
- `BinaryCodec`: Binary encoding/decoding with variant types
- `ProtocolValidator`: Multi-layer validation (checksums, MAC, version negotiation)
- `ConnectionStateMachine`: 20+ state connection state machine
- `ConnectionPool`: Sophisticated connection pool with lifecycle management
- `PacketAssembler`: Packet reassembly with fragmentation and deduplication
- `BufferPool`: DirectByteBuffer pooling for zero-copy operations
- `SessionManager`: Session management with authentication and authorization
- `CompressionLayer`: Multiple codec compression support
- `EncryptionLayer`: AES-GCM encryption with key rotation
- `RetryEngine`: Exponential backoff with circuit breakers
- `MetricsCollector`: Metrics and observability layer
- `AsyncMessageQueue`: Thread-safe async message processing

## Building

### Prerequisites

- Java 11 or higher
- Maven 3.6 or higher

### Build with Maven

```bash
mvn clean package
```

### Build for Fuzzing

```bash
cd .clusterfuzzlite
bash build.sh
```

## Usage

### Basic Example

```java
import com.aetherflow.AetherFlow;
import com.aetherflow.ProtocolMessage;

public class Example {
    public static void main(String[] args) throws Exception {
        AetherFlow protocol = new AetherFlow();
        
        // Create a message
        ProtocolMessage message = protocol.createMessage(
            ProtocolMessage.MessageType.HANDSHAKE,
            "Hello AetherFlow".getBytes()
        );
        
        // Serialize and send
        byte[] serialized = protocol.serializeMessage(message);
        
        // Deserialize received message
        ProtocolMessage received = protocol.deserializeMessage(serialized);
        
        // Process message
        protocol.processMessage(received);
        
        protocol.shutdown();
    }
}
```

### Connection Pool Usage

```java
import com.aetherflow.ConnectionPool;
import com.aetherflow.ConnectionPool.ConnectionEntry;

ConnectionPool pool = new ConnectionPool();

// Acquire connection
ConnectionEntry entry = pool.acquireConnection("localhost", 8080);

// Use connection
// ... perform operations ...

// Release connection
pool.releaseConnection(entry);
```

### Session Management

```java
import com.aetherflow.SessionManager;
import com.aetherflow.SessionManager.SessionEntry;

SessionManager sessionManager = new SessionManager();

// Create session
SessionEntry session = sessionManager.createSession("user123", "auth_token");

// Authenticate
sessionManager.authenticateSession(session.getSessionId(), "admin");

// Grant permission
sessionManager.grantPermission(session.getSessionId(), "read_data");
```

## Fuzzing

AetherFlow includes 5 Jazzer fuzzing harnesses:

- `ProtocolParserFuzzer`: Tests protocol message parsing
- `StateMachineFuzzer`: Tests connection state machine transitions
- `PacketAssemblerFuzzer`: Tests packet reassembly logic
- `SessionManagerFuzzer`: Tests session management operations
- `ConnectionPoolFuzzer`: Tests connection pool lifecycle

### Running Fuzzers

```bash
# Build for fuzzing
cd .clusterfuzzlite
bash build.sh

# Run individual fuzzer (with Jazzer)
jazzer --cp=out/aether-flow.jar:out/fuzz-harnesses.jar \
       --target_class=com.aetherflow.ProtocolParserFuzzer \
       --target_method=fuzzerTestOneInput
```

## Protocol Specification

### Message Format

```
+------------------+
| Magic Bytes (4)  | 0x4E 0x58 0x50 0x52 ("NXPR")
+------------------+
| Version (2)      | Protocol version
+------------------+
| Type (1)         | Message type
+------------------+
| Sequence (4)     | Sequence number
+------------------+
| Session ID (4)   | Session identifier
+------------------+
| Flags (4)        | Feature flags
+------------------+
| Payload Len (4)  | Payload length
+------------------+
| Checksum (4)     | CRC32 checksum
+------------------+
| Timestamp (4)    | Unix timestamp
+------------------+
| Fragment Off (4) | Fragment offset
+------------------+
| Total Size (4)   | Total fragment size
+------------------+
| Frag Index (2)   | Fragment index
+------------------+
| Total Frags (2)  | Total fragments
+------------------+
| Ext Headers (*)  | Extended headers
+------------------+
| Payload (*)      | Message payload
+------------------+
```

### Message Types

- `HANDSHAKE (0x01)`: Connection handshake
- `AUTH (0x02)`: Authentication
- `DATA (0x03)`: Data transfer
- `ACK (0x04)`: Acknowledgment
- `NACK (0x05)`: Negative acknowledgment
- `HEARTBEAT (0x06)`: Keep-alive
- `SESSION_CREATE (0x07)`: Session creation
- `SESSION_CLOSE (0x08)`: Session termination
- `KEY_ROTATION (0x09)`: Key rotation
- `COMPRESSED_DATA (0x0A)`: Compressed data
- `ENCRYPTED_DATA (0x0B)`: Encrypted data
- `FRAGMENT_START (0x0C)`: Fragment start
- `FRAGMENT_CONT (0x0D)`: Fragment continuation
- `FRAGMENT_END (0x0E)`: Fragment end
- `RETRANSMIT_REQUEST (0x0F)`: Retransmission request
- `FLOW_CONTROL (0x10)`: Flow control
- `ERROR (0x11)`: Error notification
- `VARIANT (0x12)`: Variant type
- `BATCH (0x13)`: Batched messages

## Performance

AetherFlow is designed for high-performance scenarios:

- Zero-copy operations via DirectByteBuffer pooling
- Efficient binary serialization
- Connection pooling with LRU eviction
- Async message processing with backpressure
- Configurable compression and encryption

## Security

- AES-GCM encryption for data confidentiality
- HMAC-based message authentication
- Session-based authentication and authorization
- Key rotation for forward secrecy
- Input validation and sanitization

## License

Apache License 2.0

## Version

Current version: 2.1.0
