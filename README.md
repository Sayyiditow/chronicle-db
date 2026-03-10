# chronicle-db

A high-performance, disk-persisted database built on OpenHFT's Chronicle Map and MapDB. Designed for applications requiring fast read/write operations with billions of records.

## Overview

chronicle-db provides:
- **Off-heap storage** using Chronicle Map for memory-mapped persistence
- **Secondary indexes** using MapDB for fast lookups on any field
- **Socket-based server** for distributed client-server architecture
- **Fory serialization** for high-performance data transfer
- **Replication support** for failover and high availability
- **Hot-swappable tasks** for custom operations without server restart

## Architecture

```
┌─────────────────┐     Socket      ┌─────────────────┐
│    Your App     │ ◄────────────► │  chronicle-db   │
│                 │   Fory Proto   │     Server      │
└─────────────────┘                 └────────┬────────┘
                                             │
                         ┌───────────────────┼───────────────────┐
                         │                   │                   │
                   ┌─────▼─────┐       ┌─────▼─────┐       ┌─────▼─────┐
                   │ Chronicle │       │  MapDB    │       │ Chronicle │
                   │ Map (Data)│       │ (Indexes) │       │   Queue   │
                   └───────────┘       └───────────┘       └─────┬─────┘
                                                                 │
                                                           Replication
                                                                 │
                                                           ┌─────▼─────┐
                                                           │  Standby  │
                                                           │  Server   │
                                                           └───────────┘
```

## Getting Started

### 1. Add Dependency

In your `build.gradle`:

```gradle
dependencies {
    api project(':chronicle-db')
    // or if published to maven:
    // api 'chronicle.db:chronicle-db:0.0.1-RELEASE'
}
```

### 2. Configure JVM Arguments

Chronicle Map requires special JVM flags:

```gradle
application {
    mainClass = 'chronicle.db.Server'
    applicationDefaultJvmArgs = [
        "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED",
        "--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED",
        "--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-exports=java.base/jdk.internal.util=ALL-UNNAMED",
        "--enable-native-access=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow"
    ]
}
```

### 3. Create server.properties

```properties
port=9099
dbPath=.data
dbArchPath=.data-arch/
env=dev
primary=true

# Replication (optional)
replication=false
queueSize=512
standbyDbUrls=
standbyDbPorts=
```

### 4. Set JAR Manifest

```gradle
jar {
    manifest {
        attributes(
            'Class-Path': configurations.runtimeClasspath.files.collect { it.getName() }.join(' '),
            'Main-Class': 'chronicle.db.Server'
        )
    }
}
```

## Data Types

chronicle-db enforces specific Java types for optimal storage and querying:

| Use Case | Java Type | Notes |
|----------|-----------|-------|
| Text fields (varchar, text) | `String` | Primary/foreign keys should also be String (UUID preferred) |
| Dates and timestamps | `long` | Format: `yyyyMMddHHmmss` (e.g., `20261225143000`) |
| Currency/amounts | `BigDecimal` | Never use float/double for money |
| Fixed reference values | `enum` | Must be in separate files for Fory registration |
| True/false flags | `boolean` | Primitive type |
| Small numbers | `int` | For counts, quantities |
| Precision decimals | `double` | Only if precision fits requirements |
| Arrays | `String[]`, `Object[]` | For multi-value fields |

**Important**: The map key is always a `String`. UUID is the preferred format.

## Creating Entities

Entities must implement `BytesMarshallable` (for Chronicle Map) and `IChronicle` (for subset queries):

```java
package my.app.entity;

import chronicle.db.entity.IChronicle;
import net.openhft.chronicle.bytes.BytesMarshallable;

public class User implements BytesMarshallable, IChronicle {
    // Fields - use public for Chronicle Map serialization
    public String email, firstName, lastName, password;
    public String[] roles;
    public UserStatus status;  // enum in separate file
    public boolean isAdmin;
    public long createdAt, updatedAt;

    // Required: No-arg constructor
    public User() {}

    // Full constructor
    public User(String email, String firstName, ...) {
        this.email = email;
        this.firstName = firstName;
        // ...
    }

    // IChronicle: Column headers for CSV export
    @Override
    public String[] header() {
        return new String[] { "ID", "Email", "First Name", "Last Name", "Status", "Admin" };
    }

    // IChronicle: Row data for CSV export
    @Override
    public Object[] row(String key) {
        return new Object[] { key, email, firstName, lastName, status, isAdmin };
    }

    // IChronicle: Field accessor for subset queries
    @Override
    public Object getFieldValue(String fieldName) {
        return switch (fieldName) {
            case "email" -> email;
            case "firstName" -> firstName;
            case "lastName" -> lastName;
            case "status" -> status;
            case "isAdmin" -> isAdmin;
            case "roles" -> roles != null ? roles.clone() : null;  // Clone arrays!
            case "createdAt" -> createdAt;
            case "updatedAt" -> updatedAt;
            default -> throw new IllegalArgumentException("Unknown field: " + fieldName);
        };
    }
}
```

## Creating DAOs

DAOs define how entities are stored. Implement `ChronicleDao<V>`:

```java
package my.app.dao;

import chronicle.db.dao.ChronicleDao;
import com.jsoniter.spi.TypeLiteral;
import my.app.entity.User;
import java.util.Set;

public class UserDao implements ChronicleDao<User> {
    private final String dataPath;
    private static final ThreadLocal<User> USING = ThreadLocal.withInitial(User::new);

    public UserDao(String dataPath) {
        this.dataPath = dataPath;
        createDataDirs();  // Creates /data/, /indexes/, /files/, /backup/
    }

    // Expected entries per shard (file rotation threshold)
    @Override
    public long entries() {
        return 10_000;  // New file created when exceeded
    }

    // Average key size in bytes for Chronicle Map allocation
    @Override
    public int averageKeySize() {
        return 36;  // UUID length
    }

    // Average value size for Chronicle Map allocation
    @Override
    public User averageValue() {
        return new User("email@example.com", "FirstName", "LastName", ...);
    }

    // Full data path including entity name
    @Override
    public String dataPath() {
        return dataPath + "/" + name();  // e.g., ".data/dbDir/User"
    }

    // Thread-local reusable instance for reads
    @Override
    public User using() {
        return USING.get();
    }

    // JSON type for serialization
    @Override
    public TypeLiteral<User> jsonType() {
        return new TypeLiteral<>() {};
    }

    // Optional: Secondary indexes for fast lookups
    @Override
    public Set<String> indexFileNames() {
        return Set.of("email", "status", "createdAt");
    }

    // Optional: Bloat factor for Chronicle Map (default 1.0)
    @Override
    public double bloatFactor() {
        return 1.5;  // 50% extra space for updates
    }
}
```

## Connecting as a Client

Use `ClientSocketService` to connect to the chronicle-db server:

```java
import chronicle.db.service.ClientSocketService;

// Create connection pool
ClientSocketService dbService = new ClientSocketService(
    "localhost",  // server URL
    9099,         // server port
    10,           // pool size
    60000         // socket timeout (ms)
);

// Execute queries
Map<String, Object> query = new HashMap<>();
query.put("mode", QueryMode.GET);
query.put("fqn", "my.app.dao.UserDao");
query.put("dbDir", "XYZ");
query.put("filePath", "");
query.put("key", "user-uuid-123");

Object result = dbService.execute(query);
```

## Query Modes

### Read Operations

| Mode | Description | Returns |
|------|-------------|---------|
| `GET` | Get single record by key | `V` or `null` |
| `GET_ALL` | Get multiple records by keys | `Map<String, V>` |
| `GET_SUBSET` | Get specific fields of a record | `Map<String, Object>` |
| `FETCH` | Get all records (paginated) | `Map<String, V>` |
| `FETCH_KEYS` | Get all keys | `Set<String>` |
| `SEARCH` | Search with criteria | `Map<String, V>` |
| `SEARCH_KEYS` | Search, return keys only | `Set<String>` |
| `SEARCH_COUNT` | Count matching records | `int` |
| `EXISTS` | Check if key exists | `boolean` |
| `GET_FILE` | Get file from /files/ directory | `byte[]` |

### Write Operations

| Mode | Description | Returns |
|------|-------------|---------|
| `PUT` | Insert or update | `PutStatus` |
| `INSERT` | Insert only (fails if exists) | `PutStatus` |
| `UPDATE` | Update only (fails if not exists) | `PutStatus` |
| `PUT_MULTIPLE` | Batch insert/update | `PutStatus` |
| `DELETE` | Delete by key | `boolean` |
| `DELETE_MULTIPLE` | Batch delete | `boolean` |
| `UPDATE_FILE` | Save file to /files/ | `boolean` |
| `TRUNCATE` | Delete all records | `boolean` |

### Maintenance Operations

| Mode | Description |
|------|-------------|
| `REFRESH_INDEXES` | Rebuild secondary indexes |
| `VACUUM` | Reclaim space from deleted records |
| `BACKUP` | Create backup of data files |
| `RESIZE` | Resize Chronicle Map file |
| `FAIL_OVER` | Check if safe to failover to standby |

## Search Types

The `Search` record supports multiple search operations:

```java
import chronicle.db.entity.Search;
import chronicle.db.entity.Search.SearchType;

// Exact match
new Search("status", SearchType.EQUAL, "ACTIVE")

// Not equal
new Search("status", SearchType.NOT_EQUAL, "DELETED")

// Comparisons
new Search("createdAt", SearchType.GREATER, 20261201000000L)
new Search("amount", SearchType.LESS_OR_EQUAL, new BigDecimal("1000"))

// Pattern matching (case-insensitive)
new Search("email", SearchType.CONTAINS, "gmail")
new Search("firstName", SearchType.STARTS_WITH, "John")
new Search("lastName", SearchType.ENDS_WITH, "son")
new Search("email", SearchType.LIKE, "%@company.com")

// Set operations
new Search("status", SearchType.IN, List.of("ACTIVE", "PENDING"))
new Search("status", SearchType.NOT_IN, List.of("DELETED", "ARCHIVED"))

// Range
new Search("createdAt", SearchType.BETWEEN, new Object[] { 20261201000000L, 20261231235959L })

// With limit
new Search("status", SearchType.EQUAL, "ACTIVE", 100)  // Max 100 results

// Skip index (force full scan)
new Search("status", SearchType.EQUAL, "ACTIVE", true)
```

## File Structure

Each DAO creates the following directory structure:

```
{dbPath}/{dbDir}/{EntityName}/
├── data/           # Chronicle Map data files (sharded)
│   ├── data-0      # First shard
│   ├── data-1      # Second shard (when data-0 is full)
│   └── ...
├── indexes/        # MapDB secondary indexes
│   ├── email       # Index on email field
│   ├── status      # Index on status field
│   └── keys        # Key map for routing
├── files/          # Static files (PDFs, images, etc.)
└── backup/         # Backup files
```

## Hot-Swappable Tasks

Create custom tasks that can be loaded/reloaded without server restart:

```java
package my.app.task;

import chronicle.db.service.DbTask;
import chronicle.db.service.ReplicationQueue;
import java.util.Map;

public class CleanupTask implements DbTask {
    @Override
    public Object execute(Map<String, Object> params, ReplicationQueue queue) {
        // Your task logic here
        String tenantId = params.get("tenantId").toString();
        // ... cleanup logic ...
        return "Cleanup completed for " + tenantId;
    }
}
```

Build tasks as a separate JAR:

```gradle
// In build.gradle
jar {
    exclude 'my/app/task/**'  // Exclude from main JAR
}

tasks.register('taskJar', Jar) {
    archiveBaseName = 'app-tasks'
    from(sourceSets.main.output) {
        include 'my/app/task/**'
    }
    destinationDirectory = file("build/libs/tasks")
}
```

Execute tasks via `EXECUTE_TASK` query mode:

```java
Map<String, Object> query = new HashMap<>();
query.put("mode", QueryMode.EXECUTE_TASK);
query.put("taskClass", "my.app.task.CleanupTask");
query.put("params", Map.of("tenantId", "tenant1"));

Object result = dbService.execute(query);
```

Refresh tasks without restart:

```java
query.put("mode", QueryMode.REFRESH_TASKS);
dbService.execute(query);
```

## Replication

Enable replication for high availability:

```properties
# Primary server
primary=true
replication=true
standbyDbUrls=standby1.example.com,standby2.example.com
standbyDbPorts=9099,9099

# Standby server
primary=false
```

The primary writes to a Chronicle Queue, and background threads replicate to standbys. Use `FAIL_OVER` to verify all standbys are in sync before switching.

### WAL Without Standby Server

If you want Write-Ahead Logging (WAL) for durability without running a separate standby server, you can configure the server to use itself as the standby:

```properties
# Primary server with WAL but no actual standby
primary=true
replication=false
standbyDbUrls=localhost
standbyDbPorts=9099
```

This configuration:
- Creates a primary tailer to track local database writes
- Creates a "standby" tailer pointing to itself (localhost:9099)
- Ensures all writes go through the Chronicle Queue before being applied
- Provides crash recovery - on restart, any unprocessed queue entries are replayed

The `replication=false` setting disables actual network replication to standbys while still maintaining the WAL queue. The duplicate tailer (localhost pointing to itself) is harmless since tailer names are stored in a Set, preventing actual duplicates.

This is useful for production servers that:
- Need WAL durability guarantees
- Don't have a standby server available
- Want to ensure writes are persisted before acknowledgment

### Crash Recovery Replay

After a power outage, ChronicleMap data in the OS page cache may not have been flushed to disk. To recover potentially lost writes, you can replay the last WAL cycle on startup:

```bash
java -Dchronicle.queue.replay=true -jar chronicle-db.jar
```

This will:
1. Reset all tailers to the start of the last written queue cycle
2. Replay all WAL entries from that cycle via `processPending()`
3. Re-apply operations to ChronicleMap (idempotent, safe to replay)

**Important:** Remove the `-Dchronicle.queue.replay=true` property for subsequent starts, otherwise it will replay on every startup.

## System Properties

Runtime configuration via JVM system properties:

### DAO Configuration
| Property | Description | Default |
|----------|-------------|---------|
| `chronicle.<name>.entries` | Entries per shard for a specific DAO | `1000` |
| `chronicle.<name>.key.size` | Average key size in bytes | `36` |
| `chronicle.<name>.indexes` | Comma-separated index field names | (empty) |

### Server Configuration
| Property | Description | Default |
|----------|-------------|---------|
| `chronicle.tasks.dir` | Directory for hot-swappable task JARs | `../lib/tasks` |
| `chronicle.recovery.mode` | Enable recovery mode on startup | `false` |
| `chronicle.queue.replay` | Replay last WAL cycle on startup (for crash recovery) | `false` |
| `chronicle.db.batchSizeMedium` | Batch size for medium operations | `20000` |
| `chronicle.db.batchSizeLarge` | Batch size for large operations | `50000` |
| `chronicle.search.hardLimit` | Max results returned per search query | `100000` |

### Thread Pool Configuration
| Property | Description | Default |
|----------|-------------|---------|
| `chronicle.shared.pool.size` | Shared thread pool size | `min(processors, 32)` |
| `chronicle.iterable.pool.size` | Iterable operations pool size | `min(processors, 32)` |

### Index Configuration
| Property | Description | Default |
|----------|-------------|---------|
| `chronicle.indexes.chunkSize` | Chunk size for index operations | `10000` |
| `chronicle.indexes.concurrencyScale` | MapDB index concurrency scale | `16` |
| `chronicle.indexes.nodeSize` | MapDB B+ tree node size | `32` |
| `chronicle.keyMap.concurrencyScale` | Key map concurrency scale | `16` |

### Examples

```bash
# Set entries per shard for User DAO to 50,000
java -Dchronicle.User.entries=50000 -jar server.jar

# Set average key size for Transaction DAO
java -Dchronicle.Transaction.key.size=64 -jar server.jar

# Add indexes on the fly
java -Dchronicle.User.indexes=email,status,createdDate -jar server.jar

# Configure thread pools for high-concurrency
java -Dchronicle.shared.pool.size=64 -Dchronicle.iterable.pool.size=64 -jar server.jar

# Set custom tasks directory
java -Dchronicle.tasks.dir=/opt/tasks -jar server.jar
```

DAOs can override these methods to provide different hardcoded defaults:

```java
@Override
public long entries() {
    return 50_000;  // Custom default for high-volume entity
}

@Override
public int averageKeySize() {
    return 150;  // Longer keys for this entity
}

@Override
public Set<String> indexFileNames() {
    return Set.of("email", "status", "createdDate");  // Hardcoded indexes
}
```

After changing entries, key size, or indexes, run vacuum to recreate files with new sizing.

## Best Practices

1. **Use UUIDs for keys** - Ensures uniqueness across distributed systems
2. **Size averageValue() correctly** - Oversized allocations waste memory, undersized cause resizing
3. **Index selectively** - Only index fields you search on frequently
4. **Use subset queries** - Fetch only needed fields to reduce memory
5. **Batch operations** - Use `PUT_MULTIPLE` instead of multiple `PUT` calls
6. **Monitor shard count** - Too many shards indicate entries() is set too low

## Dependencies

- Chronicle Map 2026.1 - Off-heap storage
- Chronicle Queue 2026.1 - Replication WAL
- MapDB 3.1.0 - Secondary indexes
- Fory 0.15.0 - Serialization
- JsonIter 0.9.23 - JSON parsing
- TinyLog 2.7.0 - Logging

## License

This project is open source and available under the [MIT License](LICENSE).
