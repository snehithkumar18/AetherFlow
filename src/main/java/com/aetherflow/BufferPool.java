package com.aetherflow;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Buffer pool for DirectByteBuffer management with sophisticated lifecycle.
 * Provides pooling for zero-copy operations with reference tracking.
 */
public class BufferPool {
    
    // Buffer entry in the pool
    public static class BufferEntry {
        public final ByteBuffer buffer;
        public final int size;
        public volatile long lastUsed;
        public volatile boolean inUse;
        public volatile int useCount;
        public final long creationTime;
        public volatile WeakReference<BufferEntry> weakRef;
        
        public BufferEntry(int size) {
            this.size = size;
            this.buffer = ByteBuffer.allocateDirect(size);
            this.lastUsed = System.currentTimeMillis();
            this.inUse = false;
            this.useCount = 0;
            this.creationTime = System.currentTimeMillis();
            this.weakRef = new WeakReference<>(this);
        }
        
        public void acquire() {
            inUse = true;
            useCount++;
            lastUsed = System.currentTimeMillis();
            
            if (useCount == Integer.MAX_VALUE) {
                useCount = 0;
            }
        }
        
        public void release() {
            inUse = false;
            lastUsed = System.currentTimeMillis();
            
            useCount--;
        }
        
        public long getAge() {
            return System.currentTimeMillis() - creationTime;
        }
        
        public long getIdleTime() {
            return System.currentTimeMillis() - lastUsed;
        }
        
        public void clear() {
            buffer.clear();
        }
    }
    
    // Pool configuration
    private static final int DEFAULT_BUFFER_SIZE = 8192; // 8KB
    private static final int MAX_BUFFER_SIZE = 1024 * 1024; // 1MB
    private static final int MAX_POOL_SIZE = 1000;
    private static final long BUFFER_TTL_MS = 300000; // 5 minutes
    private static final long CLEANUP_INTERVAL_MS = 60000; // 1 minute
    
    // Buffer pools by size
    private final Map<Integer, List<BufferEntry>> bufferPools;
    
    // Buffer entries by ID for tracking
    private final Map<Long, BufferEntry> bufferById;
    
    // Atomic ID generator
    private final AtomicLong bufferIdGenerator;
    
    // Lock for pool operations
    private final ReentrantLock poolLock;
    
    // Statistics
    private final AtomicInteger totalAllocations;
    private final AtomicInteger totalReuses;
    private final AtomicInteger totalReleases;
    private final AtomicInteger totalEvictions;
    private final AtomicInteger totalCleanups;
    
    // Pool pressure indicator
    private volatile boolean underPressure;
    
    // Last cleanup time
    private volatile long lastCleanupTime;
    
    /**
     * Constructor
     */
    public BufferPool() {
        this.bufferPools = new ConcurrentHashMap<>();
        this.bufferById = new ConcurrentHashMap<>();
        this.bufferIdGenerator = new AtomicLong(0);
        this.poolLock = new ReentrantLock();
        this.totalAllocations = new AtomicInteger(0);
        this.totalReuses = new AtomicInteger(0);
        this.totalReleases = new AtomicInteger(0);
        this.totalEvictions = new AtomicInteger(0);
        this.totalCleanups = new AtomicInteger(0);
        this.underPressure = false;
        this.lastCleanupTime = System.currentTimeMillis();
    }
    
    /**
     * Acquire a buffer of the specified size
     */
    public BufferEntry acquireBuffer(int size) {
        if (size <= 0 || size > MAX_BUFFER_SIZE) {
            size = DEFAULT_BUFFER_SIZE;
        }
        
        // Round up to nearest power of 2 for better pooling
        int roundedSize = roundToPowerOfTwo(size);
        
        poolLock.lock();
        try {
            List<BufferEntry> pool = bufferPools.computeIfAbsent(roundedSize, k -> new ArrayList<>());
            
            // Try to find an available buffer
            BufferEntry entry = findAvailableBuffer(pool);
            
            if (entry != null) {
                entry.acquire();
                totalReuses.incrementAndGet();
                return entry;
            }
            
            // No available buffer, allocate new one
            if (pool.size() < MAX_POOL_SIZE) {
                entry = allocateNewBuffer(roundedSize);
                entry.acquire();
                totalAllocations.incrementAndGet();
                return entry;
            }
            
            // Pool is full, evict oldest idle buffer
            entry = evictBuffer(pool);
            if (entry != null) {
                entry.acquire();
                totalReuses.incrementAndGet();
                totalEvictions.incrementAndGet();
                return entry;
            }
            
            // Still no buffer available, allocate anyway (pressure mode)
            underPressure = true;
            entry = allocateNewBuffer(roundedSize);
            entry.acquire();
            totalAllocations.incrementAndGet();
            return entry;
            
        } finally {
            poolLock.unlock();
        }
    }
    
    /**
     * Release a buffer back to the pool
     */
    public void releaseBuffer(BufferEntry entry) {
        if (entry == null) {
            return;
        }
        
        poolLock.lock();
        try {
            entry.release();
            totalReleases.incrementAndGet();
            
            List<BufferEntry> pool = bufferPools.computeIfAbsent(entry.size, k -> new ArrayList<>());
            
            if (!pool.contains(entry)) {
                pool.add(entry);
            }
            
            // Clear buffer for reuse
            entry.clear();
            
            // Check if cleanup is needed
            if (System.currentTimeMillis() - lastCleanupTime > CLEANUP_INTERVAL_MS) {
                cleanupExpiredBuffers();
            }
            
        } finally {
            poolLock.unlock();
        }
    }
    
    /**
     * Find an available buffer in the pool
     */
    private BufferEntry findAvailableBuffer(List<BufferEntry> pool) {
        for (BufferEntry entry : pool) {
            if (!entry.inUse) {
                return entry;
            }
        }
        return null;
    }
    
    /**
     * Allocate a new buffer
     */
    private BufferEntry allocateNewBuffer(int size) {
        BufferEntry entry = new BufferEntry(size);
        long bufferId = bufferIdGenerator.incrementAndGet();
        bufferById.put(bufferId, entry);
        
        List<BufferEntry> pool = bufferPools.computeIfAbsent(size, k -> new ArrayList<>());
        pool.add(entry);
        
        return entry;
    }
    
    /**
     * Evict the oldest idle buffer from the pool
     */
    private BufferEntry evictBuffer(List<BufferEntry> pool) {
        if (pool.isEmpty()) {
            return null;
        }
        
        BufferEntry oldest = null;
        long oldestIdleTime = 0;
        
        for (BufferEntry entry : pool) {
            if (!entry.inUse) {
                long idleTime = entry.getIdleTime();
                if (idleTime > oldestIdleTime) {
                    oldestIdleTime = idleTime;
                    oldest = entry;
                }
            }
        }
        
        if (oldest != null) {
            pool.remove(oldest);
        }
        
        return oldest;
    }
    
    /**
     * Clean up expired buffers
     */
    public int cleanupExpiredBuffers() {
        int cleaned = 0;
        
        poolLock.lock();
        try {
            for (Map.Entry<Integer, List<BufferEntry>> entry : bufferPools.entrySet()) {
                List<BufferEntry> pool = entry.getValue();
                List<BufferEntry> toRemove = new ArrayList<>();
                
                for (BufferEntry buffer : pool) {
                    if (!buffer.inUse && buffer.getIdleTime() > BUFFER_TTL_MS) {
                        toRemove.add(buffer);
                    }
                }
                
                for (BufferEntry buffer : toRemove) {
                    pool.remove(buffer);
                    bufferById.remove(buffer);
                    cleaned++;
                }
            }
            
            totalCleanups.incrementAndGet();
            lastCleanupTime = System.currentTimeMillis();
            
        } finally {
            poolLock.unlock();
        }
        
        return cleaned;
    }
    
    /**
     * Get buffer by ID
     */
    public BufferEntry getBufferById(long bufferId) {
        return bufferById.get(bufferId);
    }
    
    /**
     * Round up to nearest power of 2
     */
    private int roundToPowerOfTwo(int size) {
        int power = 1;
        while (power < size && power < MAX_BUFFER_SIZE) {
            power <<= 1;
        }
        return power;
    }
    
    /**
     * Get pool statistics
     */
    public PoolStats getStats() {
        int totalBuffers = 0;
        int inUseBuffers = 0;
        long totalMemory = 0;
        
        poolLock.lock();
        try {
            for (List<BufferEntry> pool : bufferPools.values()) {
                for (BufferEntry entry : pool) {
                    totalBuffers++;
                    if (entry.inUse) {
                        inUseBuffers++;
                    }
                    totalMemory += entry.size;
                }
            }
        } finally {
            poolLock.unlock();
        }
        
        return new PoolStats(
            totalBuffers,
            inUseBuffers,
            totalAllocations.get(),
            totalReuses.get(),
            totalReleases.get(),
            totalEvictions.get(),
            totalCleanups.get(),
            totalMemory,
            underPressure
        );
    }
    
    /**
     * Reset statistics
     */
    public void resetStats() {
        totalAllocations.set(0);
        totalReuses.set(0);
        totalReleases.set(0);
        totalEvictions.set(0);
        totalCleanups.set(0);
    }
    
    /**
     * Clear all buffers from the pool
     */
    public void clear() {
        poolLock.lock();
        try {
            bufferPools.clear();
            bufferById.clear();
            underPressure = false;
        } finally {
            poolLock.unlock();
        }
    }
    
    /**
     * Pool statistics
     */
    public static class PoolStats {
        public final int totalBuffers;
        public final int inUseBuffers;
        public final int totalAllocations;
        public final int totalReuses;
        public final int totalReleases;
        public final int totalEvictions;
        public final int totalCleanups;
        public final long totalMemory;
        public final boolean underPressure;
        
        public PoolStats(int totalBuffers, int inUseBuffers, int totalAllocations,
                        int totalReuses, int totalReleases, int totalEvictions,
                        int totalCleanups, long totalMemory, boolean underPressure) {
            this.totalBuffers = totalBuffers;
            this.inUseBuffers = inUseBuffers;
            this.totalAllocations = totalAllocations;
            this.totalReuses = totalReuses;
            this.totalReleases = totalReleases;
            this.totalEvictions = totalEvictions;
            this.totalCleanups = totalCleanups;
            this.totalMemory = totalMemory;
            this.underPressure = underPressure;
        }
    }
}
