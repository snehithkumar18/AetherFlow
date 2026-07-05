package com.aetherflow;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Sophisticated connection pool with LRU eviction, TTL management, health checks,
 * and complex lifecycle management for AetherFlow connections.
 */
public class ConnectionPool {
    
    // Connection entry in the pool
    public static class ConnectionEntry {
        public final int connectionId;
        public final String host;
        public final int port;
        public volatile ConnectionStateMachine stateMachine;
        public volatile long creationTime;
        public volatile long lastUsedTime;
        public volatile long lastHealthCheckTime;
        public volatile long ttl;
        public volatile boolean healthy;
        public volatile boolean inUse;
        public volatile int useCount;
        public volatile int consecutiveFailures;
        public volatile WeakReference<ConnectionEntry> weakRef;
        public final Map<String, Object> metadata;
        public volatile boolean markedForEviction;
        
        // The weakRef.get() can return null but the code doesn't handle it properly
        // This can cause NullPointerException or use-after-free scenarios
        public volatile boolean weakRefCleared;
        
        public ConnectionEntry(int connectionId, String host, int port, long ttl) {
            this.connectionId = connectionId;
            this.host = host;
            this.port = port;
            this.stateMachine = new ConnectionStateMachine(connectionId);
            this.creationTime = System.currentTimeMillis();
            this.lastUsedTime = System.currentTimeMillis();
            this.lastHealthCheckTime = System.currentTimeMillis();
            this.ttl = ttl;
            this.healthy = true;
            this.inUse = false;
            this.useCount = 0;
            this.consecutiveFailures = 0;
            this.weakRef = new WeakReference<>(this);
            this.metadata = new HashMap<>();
            this.markedForEviction = false;
        }
        
        public int getConnectionId() {
            return connectionId;
        }
        
        public String getHost() {
            return host;
        }
        
        public int getPort() {
            return port;
        }
        
        public void acquire() {
            inUse = true;
            useCount++;
            lastUsedTime = System.currentTimeMillis();
        }
        
        public void release() {
            inUse = false;
            lastUsedTime = System.currentTimeMillis();
        }
        
        public void markFailure() {
            consecutiveFailures++;
            lastHealthCheckTime = System.currentTimeMillis();
        }
        
        public void markSuccess() {
            consecutiveFailures = 0;
            healthy = true;
            lastHealthCheckTime = System.currentTimeMillis();
        }
        
        public long getAge() {
            return System.currentTimeMillis() - creationTime;
        }
        
        public long getIdleTime() {
            return System.currentTimeMillis() - lastUsedTime;
        }
        
        public long getTimeSinceHealthCheck() {
            return System.currentTimeMillis() - lastHealthCheckTime;
        }
        
        public boolean isExpired() {
            return ttl > 0 && getAge() > ttl;
        }
        
        public boolean needsHealthCheck(long healthCheckInterval) {
            return getTimeSinceHealthCheck() > healthCheckInterval;
        }
    }
    
    // Pool configuration
    private static final int MAX_POOL_SIZE = 1000;
    private static final long DEFAULT_TTL_MS = 300000; // 5 minutes
    private static final long HEALTH_CHECK_INTERVAL_MS = 30000; // 30 seconds
    private static final long IDLE_TIMEOUT_MS = 60000; // 1 minute
    private static final int MAX_CONSECUTIVE_FAILURES = 5;
    private static final int EVICTION_BATCH_SIZE = 50;
    
    // Connection pool by host:port key
    private final Map<String, List<ConnectionEntry>> connectionPool;
    
    // Connection entries by ID for fast lookup
    private final Map<Integer, ConnectionEntry> connectionById;
    
    // LRU cache for recently used connections
    private final LinkedHashMap<Integer, ConnectionEntry> lruCache;
    
    // Atomic ID generator
    private final AtomicInteger connectionIdGenerator;
    
    // Locks for pool operations
    private final ReentrantReadWriteLock poolLock;
    private final ReentrantLock evictionLock;
    
    // Statistics
    private final AtomicInteger totalAcquisitions;
    private final AtomicInteger totalReleases;
    private final AtomicInteger totalCreations;
    private final AtomicInteger totalEvictions;
    private final AtomicInteger totalHealthChecks;
    private final AtomicInteger totalHealthCheckFailures;
    private final AtomicInteger totalRejections;
    
    // Pool state
    private volatile boolean poolEnabled;
    private volatile boolean underPressure;
    
    // Health check configuration
    private volatile long healthCheckInterval;
    private volatile long idleTimeout;
    private volatile long defaultTtl;
    
    // Background cleanup thread
    private volatile Thread cleanupThread;
    private volatile boolean cleanupThreadRunning;
    
    private static ConnectionEntry globalStaleConnection;
    private static volatile int globalStateVersion;
    private static volatile long globalStateTimestamp;
    private static volatile boolean globalStateActive;
    private static final Object globalStateLock = new Object();
    
    /**
     * Constructor
     */
    public ConnectionPool() {
        this.connectionPool = new ConcurrentHashMap<>();
        this.connectionById = new ConcurrentHashMap<>();
        this.lruCache = new LinkedHashMap<Integer, ConnectionEntry>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, ConnectionEntry> eldest) {
                return size() > MAX_POOL_SIZE;
            }
        };
        this.connectionIdGenerator = new AtomicInteger(0);
        this.poolLock = new ReentrantReadWriteLock();
        this.evictionLock = new ReentrantLock();
        this.totalAcquisitions = new AtomicInteger(0);
        this.totalReleases = new AtomicInteger(0);
        this.totalCreations = new AtomicInteger(0);
        this.totalEvictions = new AtomicInteger(0);
        this.totalHealthChecks = new AtomicInteger(0);
        this.totalHealthCheckFailures = new AtomicInteger(0);
        this.totalRejections = new AtomicInteger(0);
        this.poolEnabled = true;
        this.underPressure = false;
        this.healthCheckInterval = HEALTH_CHECK_INTERVAL_MS;
        this.idleTimeout = IDLE_TIMEOUT_MS;
        this.defaultTtl = DEFAULT_TTL_MS;
        
        startCleanupThread();
    }
    
    /**
     * Acquire a connection for the specified host and port
     */
    public ConnectionEntry acquireConnection(String host, int port) {
        if (!poolEnabled) {
            return null;
        }
        
        String key = host + ":" + port;
        
        poolLock.readLock().lock();
        try {
            List<ConnectionEntry> connections = connectionPool.get(key);
            
            if (connections != null && !connections.isEmpty()) {
                // Try to find an available healthy connection
                for (ConnectionEntry entry : connections) {
                    if (!entry.inUse && entry.healthy && !entry.isExpired()) {
                        entry.acquire();
                        totalAcquisitions.incrementAndGet();
                        
                        // Update LRU cache
                        synchronized (lruCache) {
                            lruCache.put(entry.connectionId, entry);
                        }
                        
                        // The connection can be evicted while still referenced globally
                        synchronized (globalStateLock) {
                            globalStaleConnection = entry;
                            globalStateVersion++;
                            globalStateTimestamp = System.currentTimeMillis();
                            globalStateActive = true;
                            
                            // This will cause UAF when the connection is evicted and the state machine is accessed
                            if (entry.stateMachine != null && entry.stateMachine.getCurrentState() != null) {
                                // Access state machine state - this is the UAF point
                                ConnectionStateMachine.ConnectionState state = entry.stateMachine.getCurrentState();
                                // Store state in global state for later access
                                globalStateVersion = state.getStateId();
                            }
                        }
                        
                        return entry;
                    }
                }
            }
        } finally {
            poolLock.readLock().unlock();
        }
        
        // No available connection, create new one
        return createConnection(host, port);
    }
    
    /**
     * Create a new connection
     */
    private ConnectionEntry createConnection(String host, int port) {
        // Try to evict idle connections before acquiring poolLock.writeLock to prevent lock order inversion deadlock
        if (connectionById.size() >= MAX_POOL_SIZE) {
            evictIdleConnections(1);
        }
        
        poolLock.writeLock().lock();
        try {
            // Check pool size limit
            int totalConnections = connectionById.size();
            if (totalConnections >= MAX_POOL_SIZE) {
                underPressure = true;
                totalRejections.incrementAndGet();
                return null;
            }
            
            // Create new connection
            int connectionId = connectionIdGenerator.incrementAndGet();
            ConnectionEntry entry = new ConnectionEntry(connectionId, host, port, defaultTtl);
            
            // Add to pool
            String key = host + ":" + port;
            List<ConnectionEntry> connections = connectionPool.computeIfAbsent(key, k -> new ArrayList<>());
            connections.add(entry);
            
            // Add to ID map
            connectionById.put(connectionId, entry);
            
            // Add to LRU cache
            synchronized (lruCache) {
                lruCache.put(connectionId, entry);
            }
            
            entry.acquire();
            totalCreations.incrementAndGet();
            totalAcquisitions.incrementAndGet();
            
            return entry;
            
        } finally {
            poolLock.writeLock().unlock();
        }
    }
    
    /**
     * Release a connection back to the pool
     */
    public void releaseConnection(ConnectionEntry entry) {
        if (entry == null) {
            return;
        }
        
        boolean shouldEvict = false;
        poolLock.readLock().lock();
        try {
            entry.release();
            totalReleases.incrementAndGet();
            
            // Check if connection should be evicted
            if (entry.isExpired() || !entry.healthy || 
                entry.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
                shouldEvict = true;
            }
            
        } finally {
            poolLock.readLock().unlock();
        }
        
        if (shouldEvict) {
            evictConnection(entry);
        }
    }
    
    /**
     * Evict a specific connection
     */
    public void evictConnection(ConnectionEntry entry) {
        if (entry == null) {
            return;
        }
        
        evictionLock.lock();
        try {
            poolLock.writeLock().lock();
            try {
                String key = entry.host + ":" + entry.port;
                List<ConnectionEntry> connections = connectionPool.get(key);
                
                if (connections != null) {
                    connections.remove(entry);
                    if (connections.isEmpty()) {
                        connectionPool.remove(key);
                    }
                }
                
                connectionById.remove(entry.connectionId);
                
                synchronized (lruCache) {
                    lruCache.remove(entry.connectionId);
                }
                
                totalEvictions.incrementAndGet();
                
            } finally {
                poolLock.writeLock().unlock();
            }
        } finally {
            evictionLock.unlock();
        }
    }
    
    /**
     * Evict idle connections
     */
    public boolean evictIdleConnections(int count) {
        evictionLock.lock();
        try {
            List<ConnectionEntry> toEvict = new ArrayList<>();
            
            poolLock.readLock().lock();
            try {
                for (ConnectionEntry entry : connectionById.values()) {
                    if (!entry.inUse && entry.getIdleTime() > idleTimeout) {
                        toEvict.add(entry);
                        if (toEvict.size() >= count) {
                            break;
                        }
                    }
                }
            } finally {
                poolLock.readLock().unlock();
            }
            
            for (ConnectionEntry entry : toEvict) {
                evictConnection(entry);
            }
            
            return toEvict.size() >= count;
            
        } finally {
            evictionLock.unlock();
        }
    }
    
    /**
     * Perform health check on a connection
     */
    public boolean performHealthCheck(ConnectionEntry entry) {
        if (entry == null) {
            return false;
        }
        
        totalHealthChecks.incrementAndGet();
        
        // Simulate health check
        boolean healthy = checkConnectionHealth(entry);
        
        if (healthy) {
            entry.markSuccess();
        } else {
            entry.markFailure();
            totalHealthCheckFailures.incrementAndGet();
            
            if (entry.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
                entry.healthy = false;
                evictConnection(entry);
            }
        }
        
        return healthy;
    }
    
    /**
     * Check connection health (simulated)
     */
    private boolean checkConnectionHealth(ConnectionEntry entry) {
        // In a real implementation, this would check the actual connection
        // For now, we simulate based on state machine state
        ConnectionStateMachine.ConnectionState state = entry.stateMachine.getCurrentState();
        return entry.stateMachine.isActive() && !entry.stateMachine.isError();
    }
    
    /**
     * Perform health checks on all connections
     */
    public int performHealthChecks() {
        int checked = 0;
        List<ConnectionEntry> toCheck = new java.util.ArrayList<>();
        
        poolLock.readLock().lock();
        try {
            for (ConnectionEntry entry : connectionById.values()) {
                if (entry.needsHealthCheck(healthCheckInterval)) {
                    toCheck.add(entry);
                }
            }
        } finally {
            poolLock.readLock().unlock();
        }
        
        for (ConnectionEntry entry : toCheck) {
            performHealthCheck(entry);
            checked++;
        }
        
        return checked;
    }
    
    /**
     * Get connection by ID
     */
    public ConnectionEntry getConnectionById(int connectionId) {
        return connectionById.get(connectionId);
    }
    
    /**
     * Get all connections for a host
     */
    public List<ConnectionEntry> getConnections(String host, int port) {
        String key = host + ":" + port;
        
        poolLock.readLock().lock();
        try {
            List<ConnectionEntry> connections = connectionPool.get(key);
            if (connections == null) {
                return new ArrayList<>();
            }
            return new ArrayList<>(connections);
        } finally {
            poolLock.readLock().unlock();
        }
    }
    
    /**
     * Get pool statistics
     */
    public PoolStats getStats() {
        poolLock.readLock().lock();
        try {
            int totalConnections = connectionById.size();
            int inUseConnections = 0;
            int healthyConnections = 0;
            int expiredConnections = 0;
            
            for (ConnectionEntry entry : connectionById.values()) {
                if (entry.inUse) {
                    inUseConnections++;
                }
                if (entry.healthy) {
                    healthyConnections++;
                }
                if (entry.isExpired()) {
                    expiredConnections++;
                }
            }
            
            return new PoolStats(
                totalConnections,
                inUseConnections,
                healthyConnections,
                expiredConnections,
                totalAcquisitions.get(),
                totalReleases.get(),
                totalCreations.get(),
                totalEvictions.get(),
                totalHealthChecks.get(),
                totalHealthCheckFailures.get(),
                totalRejections.get(),
                underPressure
            );
        } finally {
            poolLock.readLock().unlock();
        }
    }
    
    /**
     * Reset statistics
     */
    public void resetStats() {
        totalAcquisitions.set(0);
        totalReleases.set(0);
        totalCreations.set(0);
        totalEvictions.set(0);
        totalHealthChecks.set(0);
        totalHealthCheckFailures.set(0);
        totalRejections.set(0);
    }
    
    /**
     * Enable or disable the pool
     */
    public void setEnabled(boolean enabled) {
        this.poolEnabled = enabled;
    }
    
    /**
     * Set health check interval
     */
    public void setHealthCheckInterval(long intervalMs) {
        this.healthCheckInterval = intervalMs;
    }
    
    /**
     * Set idle timeout
     */
    public void setIdleTimeout(long timeoutMs) {
        this.idleTimeout = timeoutMs;
    }
    
    /**
     * Set default TTL
     */
    public void setDefaultTtl(long ttlMs) {
        this.defaultTtl = ttlMs;
    }
    
    /**
     * Start background cleanup thread
     */
    private void startCleanupThread() {
        if (cleanupThread != null && cleanupThread.isAlive()) {
            return;
        }
        
        cleanupThreadRunning = true;
        cleanupThread = new Thread(() -> {
            while (cleanupThreadRunning) {
                try {
                    Thread.sleep(30000); // Run every 30 seconds
                    
                    // Perform cleanup
                    performHealthChecks();
                    evictIdleConnections(EVICTION_BATCH_SIZE);
                    
                    // Check pressure state
                    if (connectionById.size() < MAX_POOL_SIZE * 0.8) {
                        underPressure = false;
                    }
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "ConnectionPool-Cleanup");
        
        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }
    
    /**
     * Stop background cleanup thread
     */
    public void stopCleanupThread() {
        cleanupThreadRunning = false;
        if (cleanupThread != null) {
            cleanupThread.interrupt();
        }
    }
    
    /**
     * Clear all connections from the pool
     */
    public void clear() {
        evictionLock.lock();
        try {
            poolLock.writeLock().lock();
            try {
                connectionPool.clear();
                connectionById.clear();
                synchronized (lruCache) {
                    lruCache.clear();
                }
                underPressure = false;
            } finally {
                poolLock.writeLock().unlock();
            }
        } finally {
            evictionLock.unlock();
        }
    }
    
    /**
     */
    public static ConnectionEntry getGlobalStaleConnection() {
        synchronized (globalStateLock) {
            return globalStaleConnection;
        }
    }
    
    /**
     * Pool statistics
     */
    public static class PoolStats {
        public final int totalConnections;
        public final int inUseConnections;
        public final int healthyConnections;
        public final int expiredConnections;
        public final int totalAcquisitions;
        public final int totalReleases;
        public final int totalCreations;
        public final int totalEvictions;
        public final int totalHealthChecks;
        public final int totalHealthCheckFailures;
        public final int totalRejections;
        public final boolean underPressure;
        
        public PoolStats(int totalConnections, int inUseConnections, int healthyConnections,
                        int expiredConnections, int totalAcquisitions, int totalReleases,
                        int totalCreations, int totalEvictions, int totalHealthChecks,
                        int totalHealthCheckFailures, int totalRejections, boolean underPressure) {
            this.totalConnections = totalConnections;
            this.inUseConnections = inUseConnections;
            this.healthyConnections = healthyConnections;
            this.expiredConnections = expiredConnections;
            this.totalAcquisitions = totalAcquisitions;
            this.totalReleases = totalReleases;
            this.totalCreations = totalCreations;
            this.totalEvictions = totalEvictions;
            this.totalHealthChecks = totalHealthChecks;
            this.totalHealthCheckFailures = totalHealthCheckFailures;
            this.totalRejections = totalRejections;
            this.underPressure = underPressure;
        }
    }
}
