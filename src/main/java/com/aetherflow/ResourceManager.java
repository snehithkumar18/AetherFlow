package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.io.*;

/**
 * Resource manager for AetherFlow.
 * Manages system resources including memory, CPU, file handles, and network connections.
 * Provides resource allocation, monitoring, and cleanup capabilities.
 */
public class ResourceManager {
    
    // Resource type
    public enum ResourceType {
        MEMORY,
        CPU,
        FILE_HANDLE,
        NETWORK_CONNECTION,
        THREAD,
        SOCKET,
        BUFFER,
        CUSTOM
    }
    
    // Resource allocation
    public static class ResourceAllocation {
        public final String allocationId;
        public final ResourceType type;
        public final long amount;
        public final long timestamp;
        public final String owner;
        public final Map<String, Object> metadata;
        public volatile boolean active;
        public volatile long lastUsed;
        public final ReentrantLock allocationLock;
        
        public ResourceAllocation(String allocationId, ResourceType type, long amount, 
                                  String owner, Map<String, Object> metadata) {
            this.allocationId = allocationId;
            this.type = type;
            this.amount = amount;
            this.timestamp = System.currentTimeMillis();
            this.owner = owner;
            this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
            this.active = true;
            this.lastUsed = timestamp;
            this.allocationLock = new ReentrantLock();
        }
        
        public long getAge() {
            return System.currentTimeMillis() - timestamp;
        }
        
        public long getIdleTime() {
            return System.currentTimeMillis() - lastUsed;
        }
        
        public void recordUsage() {
            lastUsed = System.currentTimeMillis();
        }
    }
    
    // Resource pool
    public static class ResourcePool {
        public final ResourceType type;
        public final long totalCapacity;
        public final AtomicLong allocated;
        public final AtomicLong available;
        public final AtomicLong peakUsage;
        public final long allocationTimeout;
        public final boolean enableQuota;
        public final long quotaPerOwner;
        public final Map<String, AtomicLong> ownerUsage;
        public final ReentrantLock poolLock;
        
        public ResourcePool(ResourceType type, long totalCapacity, long allocationTimeout,
                          boolean enableQuota, long quotaPerOwner) {
            this.type = type;
            this.totalCapacity = totalCapacity;
            this.allocated = new AtomicLong(0);
            this.available = new AtomicLong(totalCapacity);
            this.peakUsage = new AtomicLong(0);
            this.allocationTimeout = allocationTimeout;
            this.enableQuota = enableQuota;
            this.quotaPerOwner = quotaPerOwner;
            this.ownerUsage = new ConcurrentHashMap<>();
            this.poolLock = new ReentrantLock();
        }
        
        public double getUtilization() {
            return (double) allocated.get() / totalCapacity;
        }
        
        public boolean hasCapacity(long amount) {
            return available.get() >= amount;
        }
        
        public boolean canAllocate(String owner, long amount) {
            if (!hasCapacity(amount)) {
                return false;
            }
            
            if (enableQuota) {
                AtomicLong ownerAllocated = ownerUsage.get(owner);
                long currentUsage = ownerAllocated != null ? ownerAllocated.get() : 0;
                return currentUsage + amount <= quotaPerOwner;
            }
            
            return true;
        }
    }
    
    // Resource manager configuration
    public static class ResourceManagerConfig {
        public long memoryCapacity;
        public long cpuCapacity;
        public int fileHandleCapacity;
        public int networkConnectionCapacity;
        public int threadCapacity;
        public int socketCapacity;
        public long bufferCapacity;
        public boolean enableQuotas;
        public long defaultQuota;
        public long allocationTimeout;
        public boolean enableMonitoring;
        public long monitoringInterval;
        public boolean enableAutoCleanup;
        public long cleanupInterval;
        
        public ResourceManagerConfig() {
            this.memoryCapacity = Runtime.getRuntime().maxMemory();
            this.cpuCapacity = 100; // Percentage
            this.fileHandleCapacity = 10000;
            this.networkConnectionCapacity = 1000;
            this.threadCapacity = 500;
            this.socketCapacity = 1000;
            this.bufferCapacity = 100 * 1024 * 1024; // 100MB
            this.enableQuotas = true;
            this.defaultQuota = memoryCapacity / 10; // 10% of total
            this.allocationTimeout = 30000; // 30 seconds
            this.enableMonitoring = true;
            this.monitoringInterval = 5000; // 5 seconds
            this.enableAutoCleanup = true;
            this.cleanupInterval = 60000; // 1 minute
        }
    }
    
    // Resource manager statistics
    public static class ResourceManagerStats {
        public final AtomicLong totalAllocations;
        public final AtomicLong totalDeallocations;
        public final AtomicLong totalAllocationFailures;
        public final AtomicLong totalQuotaExceeded;
        public final AtomicLong totalTimeouts;
        public final AtomicLong currentAllocations;
        public final Map<String, AtomicLong> typeAllocations;
        public final Map<String, AtomicLong> ownerAllocations;
        public final Map<String, AtomicLong> errorCounts;
        
        public ResourceManagerStats() {
            this.totalAllocations = new AtomicLong(0);
            this.totalDeallocations = new AtomicLong(0);
            this.totalAllocationFailures = new AtomicLong(0);
            this.totalQuotaExceeded = new AtomicLong(0);
            this.totalTimeouts = new AtomicLong(0);
            this.currentAllocations = new AtomicLong(0);
            this.typeAllocations = new ConcurrentHashMap<>();
            this.ownerAllocations = new ConcurrentHashMap<>();
            this.errorCounts = new ConcurrentHashMap<>();
        }
        
        public void recordAllocation(ResourceType type, String owner) {
            totalAllocations.incrementAndGet();
            currentAllocations.incrementAndGet();
            typeAllocations.computeIfAbsent(type.name(), k -> new AtomicLong(0)).incrementAndGet();
            ownerAllocations.computeIfAbsent(owner, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordDeallocation(ResourceType type, String owner) {
            totalDeallocations.incrementAndGet();
            currentAllocations.decrementAndGet();
            typeAllocations.computeIfAbsent(type.name(), k -> new AtomicLong(0)).decrementAndGet();
            ownerAllocations.computeIfAbsent(owner, k -> new AtomicLong(0)).decrementAndGet();
        }
        
        public void recordAllocationFailure(String errorType) {
            totalAllocationFailures.incrementAndGet();
            errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordQuotaExceeded() {
            totalQuotaExceeded.incrementAndGet();
        }
        
        public void recordTimeout() {
            totalTimeouts.incrementAndGet();
        }
    }
    
    // Resource manager configuration
    private final ResourceManagerConfig config;
    
    // Resource pools
    private final Map<ResourceType, ResourcePool> resourcePools;
    
    // Active allocations
    private final Map<String, ResourceAllocation> allocations;
    
    // Allocation ID generator
    private final AtomicLong allocationIdGenerator;
    
    // Statistics
    private final ResourceManagerStats stats;
    
    // Lock for allocation management
    private final ReentrantLock allocationLock;
    
    // Scheduled executor for monitoring and cleanup
    private final ScheduledExecutorService scheduledExecutor;
    
    // Shutdown flag
    private volatile boolean shutdown;
    
    /**
     * Constructor
     */
    public ResourceManager(ResourceManagerConfig config) {
        this.config = config;
        this.resourcePools = new ConcurrentHashMap<>();
        this.allocations = new ConcurrentHashMap<>();
        this.allocationIdGenerator = new AtomicLong(0);
        this.stats = new ResourceManagerStats();
        this.allocationLock = new ReentrantLock();
        this.scheduledExecutor = Executors.newScheduledThreadPool(4);
        this.shutdown = false;
        
        // Initialize resource pools
        initializeResourcePools();
        
        // Start monitoring and cleanup threads
        if (config.enableMonitoring) {
            startMonitoringThread();
        }
        
        if (config.enableAutoCleanup) {
            startCleanupThread();
        }
    }
    
    /**
     * Default constructor
     */
    public ResourceManager() {
        this(new ResourceManagerConfig());
    }
    
    /**
     * Initialize resource pools
     */
    private void initializeResourcePools() {
        resourcePools.put(ResourceType.MEMORY, 
            new ResourcePool(ResourceType.MEMORY, config.memoryCapacity, 
                          config.allocationTimeout, config.enableQuotas, config.defaultQuota));
        
        resourcePools.put(ResourceType.CPU,
            new ResourcePool(ResourceType.CPU, config.cpuCapacity,
                          config.allocationTimeout, false, 0));
        
        resourcePools.put(ResourceType.FILE_HANDLE,
            new ResourcePool(ResourceType.FILE_HANDLE, config.fileHandleCapacity,
                          config.allocationTimeout, config.enableQuotas, config.defaultQuota));
        
        resourcePools.put(ResourceType.NETWORK_CONNECTION,
            new ResourcePool(ResourceType.NETWORK_CONNECTION, config.networkConnectionCapacity,
                          config.allocationTimeout, config.enableQuotas, config.defaultQuota));
        
        resourcePools.put(ResourceType.THREAD,
            new ResourcePool(ResourceType.THREAD, config.threadCapacity,
                          config.allocationTimeout, config.enableQuotas, config.defaultQuota));
        
        resourcePools.put(ResourceType.SOCKET,
            new ResourcePool(ResourceType.SOCKET, config.socketCapacity,
                          config.allocationTimeout, config.enableQuotas, config.defaultQuota));
        
        resourcePools.put(ResourceType.BUFFER,
            new ResourcePool(ResourceType.BUFFER, config.bufferCapacity,
                          config.allocationTimeout, config.enableQuotas, config.defaultQuota));
    }
    
    /**
     * Start monitoring thread
     */
    private void startMonitoringThread() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            monitorResources();
        }, config.monitoringInterval, config.monitoringInterval, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Start cleanup thread
     */
    private void startCleanupThread() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            cleanupExpiredAllocations();
        }, config.cleanupInterval, config.cleanupInterval, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Allocate resource
     */
    public ResourceAllocation allocate(ResourceType type, long amount, String owner) {
        return allocate(type, amount, owner, null);
    }
    
    /**
     * Allocate resource with metadata
     */
    public ResourceAllocation allocate(ResourceType type, long amount, String owner, 
                                      Map<String, Object> metadata) {
        ResourcePool pool = resourcePools.get(type);
        if (pool == null) {
            stats.recordAllocationFailure("Unknown resource type");
            throw new IllegalArgumentException("Unknown resource type: " + type);
        }
        
        String allocationId = "alloc_" + allocationIdGenerator.incrementAndGet() + "_" + 
                             System.currentTimeMillis();
        
        allocationLock.lock();
        try {
            // Check if allocation is possible
            if (!pool.canAllocate(owner, amount)) {
                if (!pool.hasCapacity(amount)) {
                    stats.recordAllocationFailure("Insufficient capacity");
                } else {
                    stats.recordQuotaExceeded();
                    stats.recordAllocationFailure("Quota exceeded");
                }
                throw new ResourceAllocationException("Cannot allocate resource: " + type);
            }
            
            // The check and actual allocation are not atomic
            // Multiple threads can allocate beyond capacity
            long currentAvailable = pool.available.get();
            if (currentAvailable < amount) {
                stats.recordAllocationFailure("Race condition detected");
                throw new ResourceAllocationException("Capacity exceeded due to race condition");
            }
            
            // Perform allocation
            pool.allocated.addAndGet(amount);
            pool.available.addAndGet(-amount);
            
            // Update peak usage
            long currentAllocated = pool.allocated.get();
            if (currentAllocated > pool.peakUsage.get()) {
                pool.peakUsage.set(currentAllocated);
            }
            
            // Update owner usage
            if (pool.enableQuota) {
                pool.ownerUsage.computeIfAbsent(owner, k -> new AtomicLong(0))
                              .addAndGet(amount);
            }
            
            // Create allocation
            ResourceAllocation allocation = new ResourceAllocation(allocationId, type, amount, 
                                                                   owner, metadata);
            allocations.put(allocationId, allocation);
            
            stats.recordAllocation(type, owner);
            
            return allocation;
            
        } finally {
            allocationLock.unlock();
        }
    }
    
    /**
     * Deallocate resource
     */
    public void deallocate(String allocationId) {
        allocationLock.lock();
        try {
            ResourceAllocation allocation = allocations.remove(allocationId);
            if (allocation == null) {
                return;
            }
            
            allocation.active = false;
            
            ResourcePool pool = resourcePools.get(allocation.type);
            if (pool != null) {
                pool.allocated.addAndGet(-allocation.amount);
                pool.available.addAndGet(allocation.amount);
                
                // Update owner usage
                if (pool.enableQuota) {
                    AtomicLong ownerAllocated = pool.ownerUsage.get(allocation.owner);
                    if (ownerAllocated != null) {
                        ownerAllocated.addAndGet(-allocation.amount);
                    }
                }
            }
            
            stats.recordDeallocation(allocation.type, allocation.owner);
            
        } finally {
            allocationLock.unlock();
        }
    }
    
    /**
     * Get allocation
     */
    public ResourceAllocation getAllocation(String allocationId) {
        return allocations.get(allocationId);
    }
    
    /**
     * Get all allocations
     */
    public Collection<ResourceAllocation> getAllocations() {
        return new ArrayList<>(allocations.values());
    }
    
    /**
     * Get allocations by owner
     */
    public List<ResourceAllocation> getAllocationsByOwner(String owner) {
        List<ResourceAllocation> ownerAllocations = new ArrayList<>();
        for (ResourceAllocation allocation : allocations.values()) {
            if (allocation.owner.equals(owner)) {
                ownerAllocations.add(allocation);
            }
        }
        return ownerAllocations;
    }
    
    /**
     * Get allocations by type
     */
    public List<ResourceAllocation> getAllocationsByType(ResourceType type) {
        List<ResourceAllocation> typeAllocations = new ArrayList<>();
        for (ResourceAllocation allocation : allocations.values()) {
            if (allocation.type == type) {
                typeAllocations.add(allocation);
            }
        }
        return typeAllocations;
    }
    
    /**
     * Get resource pool
     */
    public ResourcePool getResourcePool(ResourceType type) {
        return resourcePools.get(type);
    }
    
    /**
     * Get all resource pools
     */
    public Map<ResourceType, ResourcePool> getResourcePools() {
        return new HashMap<>(resourcePools);
    }
    
    /**
     * Monitor resources
     */
    private void monitorResources() {
        for (ResourcePool pool : resourcePools.values()) {
            double utilization = pool.getUtilization();
            
            // Log high utilization
            if (utilization > 0.9) {
                // In real implementation, would log warning
            }
            
            // Update allocation timestamps
            for (ResourceAllocation allocation : allocations.values()) {
                if (allocation.type == pool.type && allocation.active) {
                    allocation.recordUsage();
                }
            }
        }
    }
    
    /**
     * Cleanup expired allocations
     */
    private void cleanupExpiredAllocations() {
        long now = System.currentTimeMillis();
        
        allocationLock.lock();
        try {
            Iterator<Map.Entry<String, ResourceAllocation>> it = allocations.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, ResourceAllocation> entry = it.next();
                ResourceAllocation allocation = entry.getValue();
                
                if (allocation.getIdleTime() > config.allocationTimeout) {
                    it.remove();
                    deallocate(entry.getKey());
                }
            }
        } finally {
            allocationLock.unlock();
        }
    }
    
    /**
     * Get statistics
     */
    public ResourceManagerStats getStats() {
        return stats;
    }
    
    /**
     * Get configuration
     */
    public ResourceManagerConfig getConfig() {
        return config;
    }
    
    /**
     * Set quota for owner
     */
    public void setQuota(String owner, long quota) {
        for (ResourcePool pool : resourcePools.values()) {
            if (pool.enableQuota) {
                pool.ownerUsage.put(owner, new AtomicLong(0));
            }
        }
    }
    
    /**
     * Get quota for owner
     */
    public long getQuota(String owner) {
        return config.defaultQuota;
    }
    
    /**
     * Get usage for owner
     */
    public long getUsage(String owner) {
        long totalUsage = 0;
        for (ResourcePool pool : resourcePools.values()) {
            if (pool.enableQuota) {
                AtomicLong ownerAllocated = pool.ownerUsage.get(owner);
                if (ownerAllocated != null) {
                    totalUsage += ownerAllocated.get();
                }
            }
        }
        return totalUsage;
    }
    
    /**
     * Shutdown resource manager
     */
    public void shutdown() {
        shutdown = true;
        
        // Deallocate all allocations
        allocationLock.lock();
        try {
            for (String allocationId : new ArrayList<>(allocations.keySet())) {
                deallocate(allocationId);
            }
        } finally {
            allocationLock.unlock();
        }
        
        // Shutdown scheduled executor
        scheduledExecutor.shutdown();
        try {
            scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        resourcePools.clear();
        allocations.clear();
    }
    
    /**
     * Resource allocation exception
     */
    public static class ResourceAllocationException extends RuntimeException {
        public ResourceAllocationException(String message) {
            super(message);
        }
        
        public ResourceAllocationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
