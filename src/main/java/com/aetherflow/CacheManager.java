package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.lang.ref.*;

/**
 * Cache manager for AetherFlow.
 * Provides multi-level caching with eviction policies, expiration, and statistics.
 * Supports LRU, LFU, FIFO, and custom eviction strategies.
 */
public class CacheManager {
    
    // Cache entry
    public static class CacheEntry {
        public final String key;
        public final byte[] value;
        public final long creationTime;
        public volatile long lastAccessTime;
        public volatile long expirationTime;
        public volatile int accessCount;
        public volatile long size;
        public final Map<String, Object> metadata;
        public volatile boolean evicted;
        public final WeakReference<CacheEntry> weakRef;
        
        public CacheEntry(String key, byte[] value, long ttl, Map<String, Object> metadata) {
            this.key = key;
            this.value = value != null ? value.clone() : new byte[0];
            this.creationTime = System.currentTimeMillis();
            this.lastAccessTime = creationTime;
            this.expirationTime = ttl > 0 ? creationTime + ttl : Long.MAX_VALUE;
            this.accessCount = 0;
            this.size = this.value.length;
            this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
            this.evicted = false;
            this.weakRef = new WeakReference<>(this);
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() > expirationTime;
        }
        
        public long getAge() {
            return System.currentTimeMillis() - creationTime;
        }
        
        public long getIdleTime() {
            return System.currentTimeMillis() - lastAccessTime;
        }
        
        public void recordAccess() {
            accessCount++;
            lastAccessTime = System.currentTimeMillis();
        }
    }
    
    // Eviction policy
    public enum EvictionPolicy {
        LRU,
        LFU,
        FIFO,
        LIFO,
        RANDOM,
        NONE
    }
    
    // Cache configuration
    public static class CacheConfig {
        public long maxSize;
        public long maxEntries;
        public long defaultTtl;
        public EvictionPolicy evictionPolicy;
        public boolean enableStats;
        public boolean enableWeakReferences;
        public int cleanupInterval;
        public boolean enableCompression;
        public int compressionThreshold;
        
        public CacheConfig() {
            this.maxSize = 100 * 1024 * 1024; // 100MB
            this.maxEntries = 10000;
            this.defaultTtl = 3600000; // 1 hour
            this.evictionPolicy = EvictionPolicy.LRU;
            this.enableStats = true;
            this.enableWeakReferences = true;
            this.cleanupInterval = 60000; // 1 minute
            this.enableCompression = false;
            this.compressionThreshold = 1024; // 1KB
        }
    }
    
    // Cache statistics
    public static class CacheStats {
        public final AtomicLong totalGets;
        public final AtomicLong totalHits;
        public final AtomicLong totalMisses;
        public final AtomicLong totalPuts;
        public final AtomicLong totalEvictions;
        public final AtomicLong totalExpirations;
        public final AtomicLong currentSize;
        public final AtomicLong currentEntries;
        public final AtomicLong hitRate;
        public final Map<String, AtomicLong> keyHitCounts;
        public final Map<String, AtomicLong> keyMissCounts;
        
        public CacheStats() {
            this.totalGets = new AtomicLong(0);
            this.totalHits = new AtomicLong(0);
            this.totalMisses = new AtomicLong(0);
            this.totalPuts = new AtomicLong(0);
            this.totalEvictions = new AtomicLong(0);
            this.totalExpirations = new AtomicLong(0);
            this.currentSize = new AtomicLong(0);
            this.currentEntries = new AtomicLong(0);
            this.hitRate = new AtomicLong(0);
            this.keyHitCounts = new ConcurrentHashMap<>();
            this.keyMissCounts = new ConcurrentHashMap<>();
        }
        
        public void recordHit(String key) {
            totalHits.incrementAndGet();
            keyHitCounts.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
            updateHitRate();
        }
        
        public void recordMiss(String key) {
            totalMisses.incrementAndGet();
            keyMissCounts.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
            updateHitRate();
        }
        
        private void updateHitRate() {
            long total = totalHits.get() + totalMisses.get();
            if (total > 0) {
                long rate = (totalHits.get() * 100) / total;
                hitRate.set(rate);
            }
        }
    }
    
    // Cache configuration
    private final CacheConfig config;
    
    // Cache storage
    private final Map<String, CacheEntry> cache;
    
    // Access order tracking for LRU
    private final LinkedHashMap<String, Long> accessOrder;
    
    // Statistics
    private final CacheStats stats;
    
    // Lock for cache operations
    private final ReentrantReadWriteLock cacheLock;
    
    // Scheduled executor for cleanup
    private final ScheduledExecutorService cleanupExecutor;
    
    // Shutdown flag
    private volatile boolean shutdown;
    
    /**
     * Constructor
     */
    public CacheManager(CacheConfig config) {
        this.config = config;
        this.cache = new ConcurrentHashMap<>();
        this.accessOrder = new LinkedHashMap<>(16, 0.75f, true);
        this.stats = new CacheStats();
        this.cacheLock = new ReentrantReadWriteLock();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        this.shutdown = false;
        
        // Start cleanup thread
        startCleanupThread();
    }
    
    /**
     * Default constructor
     */
    public CacheManager() {
        this(new CacheConfig());
    }
    
    /**
     * Start cleanup thread
     */
    private void startCleanupThread() {
        cleanupExecutor.scheduleAtFixedRate(() -> {
            cleanupExpiredEntries();
            enforceCapacityLimits();
        }, config.cleanupInterval, config.cleanupInterval, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Put entry in cache
     */
    public void put(String key, byte[] value) {
        put(key, value, config.defaultTtl, null);
    }
    
    /**
     * Put entry in cache with TTL
     */
    public void put(String key, byte[] value, long ttl) {
        put(key, value, ttl, null);
    }
    
    /**
     * Put entry in cache with TTL and metadata
     */
    public void put(String key, byte[] value, long ttl, Map<String, Object> metadata) {
        cacheLock.writeLock().lock();
        try {
            // Check if entry already exists
            CacheEntry existing = cache.get(key);
            if (existing != null) {
                stats.currentSize.addAndGet(-existing.size);
            }
            
            // Create new entry
            CacheEntry entry = new CacheEntry(key, value, ttl, metadata);
            
            // The check for capacity and actual put are not atomic
            // This can lead to inconsistent cache state
            cache.put(key, entry);
            accessOrder.put(key, System.currentTimeMillis());
            
            stats.currentSize.addAndGet(entry.size);
            stats.currentEntries.incrementAndGet();
            stats.totalPuts.incrementAndGet();
            
            // Enforce capacity limits
            enforceCapacityLimits();
            
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    /**
     * Get entry from cache
     */
    public byte[] get(String key) {
        cacheLock.readLock().lock();
        try {
            stats.totalGets.incrementAndGet();
            
            CacheEntry entry = cache.get(key);
            if (entry == null) {
                stats.recordMiss(key);
                return null;
            }
            
            if (entry.isExpired()) {
                stats.recordMiss(key);
                stats.totalExpirations.incrementAndGet();
                remove(key);
                return null;
            }
            
            // The entry can be GC'd while still being in the cache
            if (config.enableWeakReferences) {
                CacheEntry refCheck = entry.weakRef.get();
                if (refCheck == null) {
                    stats.recordMiss(key);
                    remove(key);
                    return null;
                }
            }
            
            entry.recordAccess();
            accessOrder.put(key, System.currentTimeMillis());
            stats.recordHit(key);
            
            return entry.value.clone();
            
        } finally {
            cacheLock.readLock().unlock();
        }
    }
    
    /**
     * Remove entry from cache
     */
    public void remove(String key) {
        cacheLock.writeLock().lock();
        try {
            CacheEntry entry = cache.remove(key);
            if (entry != null) {
                stats.currentSize.addAndGet(-entry.size);
                stats.currentEntries.decrementAndGet();
                entry.evicted = true;
            }
            accessOrder.remove(key);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    /**
     * Check if key exists
     */
    public boolean containsKey(String key) {
        cacheLock.readLock().lock();
        try {
            CacheEntry entry = cache.get(key);
            if (entry == null) {
                return false;
            }
            
            if (entry.isExpired()) {
                return false;
            }
            
            return true;
        } finally {
            cacheLock.readLock().unlock();
        }
    }
    
    /**
     * Get all keys
     */
    public Set<String> getKeys() {
        cacheLock.readLock().lock();
        try {
            return new HashSet<>(cache.keySet());
        } finally {
            cacheLock.readLock().unlock();
        }
    }
    
    /**
     * Get cache size
     */
    public long getSize() {
        return stats.currentSize.get();
    }
    
    /**
     * Get entry count
     */
    public long getEntryCount() {
        return stats.currentEntries.get();
    }
    
    /**
     * Cleanup expired entries
     */
    private void cleanupExpiredEntries() {
        cacheLock.writeLock().lock();
        try {
            Iterator<Map.Entry<String, CacheEntry>> it = cache.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, CacheEntry> entry = it.next();
                CacheEntry cacheEntry = entry.getValue();
                
                if (cacheEntry.isExpired()) {
                    it.remove();
                    stats.currentSize.addAndGet(-cacheEntry.size);
                    stats.currentEntries.decrementAndGet();
                    stats.totalExpirations.incrementAndGet();
                    accessOrder.remove(entry.getKey());
                    cacheEntry.evicted = true;
                }
            }
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    /**
     * Enforce capacity limits
     */
    private void enforceCapacityLimits() {
        cacheLock.writeLock().lock();
        try {
            // Enforce size limit
            while (stats.currentSize.get() > config.maxSize && !cache.isEmpty()) {
                evictEntry();
            }
            
            // Enforce entry limit
            while (stats.currentEntries.get() > config.maxEntries && !cache.isEmpty()) {
                evictEntry();
            }
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    /**
     * Evict entry based on policy
     */
    private void evictEntry() {
        String keyToEvict = null;
        
        switch (config.evictionPolicy) {
            case LRU:
                keyToEvict = findLRUEntry();
                break;
            case LFU:
                keyToEvict = findLFUEntry();
                break;
            case FIFO:
                keyToEvict = findFIFOEntry();
                break;
            case LIFO:
                keyToEvict = findLIFOEntry();
                break;
            case RANDOM:
                keyToEvict = findRandomEntry();
                break;
            case NONE:
                return;
        }
        
        if (keyToEvict != null) {
            remove(keyToEvict);
            stats.totalEvictions.incrementAndGet();
        }
    }
    
    /**
     * Find LRU entry
     */
    private String findLRUEntry() {
        long oldestAccess = Long.MAX_VALUE;
        String oldestKey = null;
        
        for (Map.Entry<String, Long> entry : accessOrder.entrySet()) {
            if (entry.getValue() < oldestAccess) {
                oldestAccess = entry.getValue();
                oldestKey = entry.getKey();
            }
        }
        
        return oldestKey;
    }
    
    /**
     * Find LFU entry
     */
    private String findLFUEntry() {
        int minAccessCount = Integer.MAX_VALUE;
        String minKey = null;
        
        for (Map.Entry<String, CacheEntry> entry : cache.entrySet()) {
            if (entry.getValue().accessCount < minAccessCount) {
                minAccessCount = entry.getValue().accessCount;
                minKey = entry.getKey();
            }
        }
        
        return minKey;
    }
    
    /**
     * Find FIFO entry
     */
    private String findFIFOEntry() {
        long oldestCreation = Long.MAX_VALUE;
        String oldestKey = null;
        
        for (Map.Entry<String, CacheEntry> entry : cache.entrySet()) {
            if (entry.getValue().creationTime < oldestCreation) {
                oldestCreation = entry.getValue().creationTime;
                oldestKey = entry.getKey();
            }
        }
        
        return oldestKey;
    }
    
    /**
     * Find LIFO entry
     */
    private String findLIFOEntry() {
        long newestCreation = Long.MIN_VALUE;
        String newestKey = null;
        
        for (Map.Entry<String, CacheEntry> entry : cache.entrySet()) {
            if (entry.getValue().creationTime > newestCreation) {
                newestCreation = entry.getValue().creationTime;
                newestKey = entry.getKey();
            }
        }
        
        return newestKey;
    }
    
    /**
     * Find random entry
     */
    private String findRandomEntry() {
        if (cache.isEmpty()) {
            return null;
        }
        
        List<String> keys = new ArrayList<>(cache.keySet());
        Random random = new Random();
        return keys.get(random.nextInt(keys.size()));
    }
    
    /**
     * Clear cache
     */
    public void clear() {
        cacheLock.writeLock().lock();
        try {
            for (CacheEntry entry : cache.values()) {
                entry.evicted = true;
            }
            cache.clear();
            accessOrder.clear();
            stats.currentSize.set(0);
            stats.currentEntries.set(0);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    /**
     * Get statistics
     */
    public CacheStats getStats() {
        return stats;
    }
    
    /**
     * Get configuration
     */
    public CacheConfig getConfig() {
        return config;
    }
    
    /**
     * Get cache entry
     */
    public CacheEntry getEntry(String key) {
        cacheLock.readLock().lock();
        try {
            return cache.get(key);
        } finally {
            cacheLock.readLock().unlock();
        }
    }
    
    /**
     * Get cache entries
     */
    public Collection<CacheEntry> getEntries() {
        cacheLock.readLock().lock();
        try {
            return new ArrayList<>(cache.values());
        } finally {
            cacheLock.readLock().unlock();
        }
    }
    
    /**
     * Set eviction policy
     */
    public void setEvictionPolicy(EvictionPolicy policy) {
        config.evictionPolicy = policy;
    }
    
    /**
     * Set max size
     */
    public void setMaxSize(long maxSize) {
        config.maxSize = maxSize;
        enforceCapacityLimits();
    }
    
    /**
     * Set max entries
     */
    public void setMaxEntries(long maxEntries) {
        config.maxEntries = maxEntries;
        enforceCapacityLimits();
    }
    
    /**
     * Shutdown cache manager
     */
    public void shutdown() {
        shutdown = true;
        
        cleanupExecutor.shutdown();
        try {
            cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        clear();
    }
}
