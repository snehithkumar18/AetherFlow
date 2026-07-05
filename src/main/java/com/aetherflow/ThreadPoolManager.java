package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.*;

/**
 * Thread pool manager for AetherFlow.
 * Manages multiple thread pools with different priorities and configurations.
 * Supports dynamic scaling, task queuing, and comprehensive monitoring.
 */
public class ThreadPoolManager {
    
    // Thread pool types
    public enum PoolType {
        IO_BOUND,
        CPU_BOUND,
        COMPUTE_INTENSIVE,
        LOW_PRIORITY,
        HIGH_PRIORITY,
        BACKGROUND,
        SCHEDULED
    }
    
    // Task priority
    public enum TaskPriority {
        LOW(1),
        NORMAL(5),
        HIGH(10),
        URGENT(20);
        
        private final int value;
        
        TaskPriority(int value) {
            this.value = value;
        }
        
        public int getValue() {
            return value;
        }
    }
    
    // Thread pool configuration
    public static class PoolConfig {
        public int corePoolSize;
        public int maxPoolSize;
        public long keepAliveTime;
        public TimeUnit timeUnit;
        public int queueCapacity;
        public boolean allowCoreThreadTimeout;
        public boolean rejectOnOverflow;
        public String poolName;
        public ThreadFactory threadFactory;
        public RejectedExecutionHandler rejectionHandler;
        
        public PoolConfig() {
            this.corePoolSize = Runtime.getRuntime().availableProcessors();
            this.maxPoolSize = Runtime.getRuntime().availableProcessors() * 2;
            this.keepAliveTime = 60;
            this.timeUnit = TimeUnit.SECONDS;
            this.queueCapacity = 1000;
            this.allowCoreThreadTimeout = false;
            this.rejectOnOverflow = false;
            this.poolName = "default";
            this.threadFactory = Executors.defaultThreadFactory();
            this.rejectionHandler = new ThreadPoolExecutor.AbortPolicy();
        }
    }
    
    // Priority task wrapper
    private static class PriorityTask implements Runnable, Comparable<PriorityTask> {
        private final Runnable task;
        private final TaskPriority priority;
        private final long submitTime;
        private final String taskId;
        
        public PriorityTask(Runnable task, TaskPriority priority, String taskId) {
            this.task = task;
            this.priority = priority;
            this.submitTime = System.currentTimeMillis();
            this.taskId = taskId;
        }
        
        @Override
        public void run() {
            task.run();
        }
        
        @Override
        public int compareTo(PriorityTask other) {
            // Higher priority first
            int priorityCompare = Integer.compare(other.priority.getValue(), this.priority.getValue());
            if (priorityCompare != 0) {
                return priorityCompare;
            }
            // FIFO for same priority
            return Long.compare(this.submitTime, other.submitTime);
        }
        
        public String getTaskId() {
            return taskId;
        }
    }
    
    // Thread pool statistics
    public static class PoolStats {
        public final AtomicLong totalTasksSubmitted;
        public final AtomicLong totalTasksCompleted;
        public final AtomicLong totalTasksRejected;
        public final AtomicLong totalTasksFailed;
        public final AtomicLong currentQueueSize;
        public final AtomicLong activeThreads;
        public final AtomicLong totalExecutionTime;
        public final AtomicLong averageExecutionTime;
        public final Map<String, AtomicLong> taskTypeCounts;
        public final Map<String, AtomicLong> errorCounts;
        
        public PoolStats() {
            this.totalTasksSubmitted = new AtomicLong(0);
            this.totalTasksCompleted = new AtomicLong(0);
            this.totalTasksRejected = new AtomicLong(0);
            this.totalTasksFailed = new AtomicLong(0);
            this.currentQueueSize = new AtomicLong(0);
            this.activeThreads = new AtomicLong(0);
            this.totalExecutionTime = new AtomicLong(0);
            this.averageExecutionTime = new AtomicLong(0);
            this.taskTypeCounts = new ConcurrentHashMap<>();
            this.errorCounts = new ConcurrentHashMap<>();
        }
        
        public void recordTaskSubmission(String taskType) {
            totalTasksSubmitted.incrementAndGet();
            taskTypeCounts.computeIfAbsent(taskType, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordTaskCompletion(long executionTime) {
            totalTasksCompleted.incrementAndGet();
            totalExecutionTime.addAndGet(executionTime);
            
            long completed = totalTasksCompleted.get();
            long newAvg = totalExecutionTime.get() / completed;
            averageExecutionTime.set(newAvg);
        }
        
        public void recordTaskRejection() {
            totalTasksRejected.incrementAndGet();
        }
        
        public void recordTaskFailure(String errorType) {
            totalTasksFailed.incrementAndGet();
            errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
        }
    }
    
    // Thread pool wrapper
    private static class ThreadPoolWrapper {
        public final ThreadPoolExecutor executor;
        public final PoolConfig config;
        public final PoolStats stats;
        public final PoolType type;
        public final AtomicLong taskIdGenerator;
        public final Map<String, Long> taskStartTimes;
        public final ReentrantLock taskLock;
        
        public ThreadPoolWrapper(PoolConfig config, PoolType type) {
            this.config = config;
            this.type = type;
            this.stats = new PoolStats();
            this.taskIdGenerator = new AtomicLong(0);
            this.taskStartTimes = new ConcurrentHashMap<>();
            this.taskLock = new ReentrantLock();
            
            // Create thread pool based on type
            BlockingQueue<Runnable> workQueue;
            if (type == PoolType.SCHEDULED) {
                workQueue = new LinkedBlockingQueue<>();
            } else {
                workQueue = new PriorityBlockingQueue<>(config.queueCapacity);
            }
            
            this.executor = new ThreadPoolExecutor(
                config.corePoolSize,
                config.maxPoolSize,
                config.keepAliveTime,
                config.timeUnit,
                workQueue,
                config.threadFactory,
                config.rejectionHandler
            );
            
            executor.allowCoreThreadTimeOut(config.allowCoreThreadTimeout);
        }
    }
    
    // Thread pool configurations
    private final Map<PoolType, PoolConfig> poolConfigs;
    
    // Thread pool wrappers
    private final Map<PoolType, ThreadPoolWrapper> threadPools;
    
    // Scheduled executor service
    private final ScheduledThreadPoolExecutor scheduledExecutor;
    
    // Lock for pool management
    private final ReentrantLock poolLock;
    
    // Shutdown flag
    private volatile boolean shutdown;
    
    // Task completion callback
    private volatile Consumer<TaskResult> taskCompletionCallback;
    
    // Task result
    public static class TaskResult {
        public final String taskId;
        public final boolean success;
        public final long executionTime;
        public final Throwable error;
        
        public TaskResult(String taskId, boolean success, long executionTime, Throwable error) {
            this.taskId = taskId;
            this.success = success;
            this.executionTime = executionTime;
            this.error = error;
        }
    }
    
    /**
     * Constructor
     */
    public ThreadPoolManager() {
        this.poolConfigs = new ConcurrentHashMap<>();
        this.threadPools = new ConcurrentHashMap<>();
        this.scheduledExecutor = new ScheduledThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors());
        this.poolLock = new ReentrantLock();
        this.shutdown = false;
        
        // Initialize default pools
        initializeDefaultPools();
    }
    
    /**
     * Initialize default thread pools
     */
    private void initializeDefaultPools() {
        // IO bound pool
        PoolConfig ioConfig = new PoolConfig();
        ioConfig.corePoolSize = Runtime.getRuntime().availableProcessors() * 2;
        ioConfig.maxPoolSize = Runtime.getRuntime().availableProcessors() * 4;
        ioConfig.poolName = "io-bound";
        poolConfigs.put(PoolType.IO_BOUND, ioConfig);
        
        // CPU bound pool
        PoolConfig cpuConfig = new PoolConfig();
        cpuConfig.corePoolSize = Runtime.getRuntime().availableProcessors();
        cpuConfig.maxPoolSize = Runtime.getRuntime().availableProcessors();
        cpuConfig.poolName = "cpu-bound";
        poolConfigs.put(PoolType.CPU_BOUND, cpuConfig);
        
        // Compute intensive pool
        PoolConfig computeConfig = new PoolConfig();
        computeConfig.corePoolSize = 1;
        computeConfig.maxPoolSize = Runtime.getRuntime().availableProcessors();
        computeConfig.poolName = "compute-intensive";
        poolConfigs.put(PoolType.COMPUTE_INTENSIVE, computeConfig);
        
        // Low priority pool
        PoolConfig lowPriorityConfig = new PoolConfig();
        lowPriorityConfig.corePoolSize = 2;
        lowPriorityConfig.maxPoolSize = 4;
        lowPriorityConfig.poolName = "low-priority";
        poolConfigs.put(PoolType.LOW_PRIORITY, lowPriorityConfig);
        
        // High priority pool
        PoolConfig highPriorityConfig = new PoolConfig();
        highPriorityConfig.corePoolSize = Runtime.getRuntime().availableProcessors();
        highPriorityConfig.maxPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        highPriorityConfig.poolName = "high-priority";
        poolConfigs.put(PoolType.HIGH_PRIORITY, highPriorityConfig);
        
        // Background pool
        PoolConfig backgroundConfig = new PoolConfig();
        backgroundConfig.corePoolSize = 2;
        backgroundConfig.maxPoolSize = 4;
        backgroundConfig.poolName = "background";
        poolConfigs.put(PoolType.BACKGROUND, backgroundConfig);
        
        // Create thread pools
        for (Map.Entry<PoolType, PoolConfig> entry : poolConfigs.entrySet()) {
            createThreadPool(entry.getKey(), entry.getValue());
        }
    }
    
    /**
     * Create thread pool
     */
    private void createThreadPool(PoolType type, PoolConfig config) {
        ThreadPoolWrapper wrapper = new ThreadPoolWrapper(config, type);
        threadPools.put(type, wrapper);
    }
    
    /**
     * Submit task to thread pool
     */
    public Future<?> submit(PoolType poolType, Runnable task) {
        return submit(poolType, task, TaskPriority.NORMAL, null);
    }
    
    /**
     * Submit task with priority
     */
    public Future<?> submit(PoolType poolType, Runnable task, TaskPriority priority) {
        return submit(poolType, task, priority, null);
    }
    
    /**
     * Submit task with priority and task type
     */
    public Future<?> submit(PoolType poolType, Runnable task, TaskPriority priority, String taskType) {
        ThreadPoolWrapper wrapper = threadPools.get(poolType);
        if (wrapper == null) {
            throw new IllegalArgumentException("Unknown pool type: " + poolType);
        }
        
        String taskId = "task_" + wrapper.taskIdGenerator.incrementAndGet() + "_" + 
                       System.currentTimeMillis();
        
        PriorityTask priorityTask = new PriorityTask(() -> {
            long startTime = System.currentTimeMillis();
            wrapper.taskStartTimes.put(taskId, startTime);
            
            try {
                task.run();
            } catch (Exception e) {
                wrapper.stats.recordTaskFailure(e.getClass().getSimpleName());
                throw e;
            } finally {
                long executionTime = System.currentTimeMillis() - startTime;
                wrapper.stats.recordTaskCompletion(executionTime);
                wrapper.taskStartTimes.remove(taskId);
                
                if (taskCompletionCallback != null) {
                    TaskResult result = new TaskResult(taskId, true, executionTime, null);
                    taskCompletionCallback.accept(result);
                }
            }
        }, priority, taskId);
        
        wrapper.stats.recordTaskSubmission(taskType != null ? taskType : "default");
        
        try {
            Future<?> future = wrapper.executor.submit(priorityTask);
            wrapper.stats.currentQueueSize.set(wrapper.executor.getQueue().size());
            wrapper.stats.activeThreads.set(wrapper.executor.getActiveCount());
            return future;
        } catch (RejectedExecutionException e) {
            wrapper.stats.recordTaskRejection();
            throw e;
        }
    }
    
    /**
     * Submit callable task
     */
    public <T> Future<T> submit(PoolType poolType, Callable<T> task) {
        return submit(poolType, task, TaskPriority.NORMAL, null);
    }
    
    /**
     * Submit callable task with priority
     */
    public <T> Future<T> submit(PoolType poolType, Callable<T> task, TaskPriority priority) {
        return submit(poolType, task, priority, null);
    }
    
    /**
     * Submit callable task with priority and task type
     */
    public <T> Future<T> submit(PoolType poolType, Callable<T> task, TaskPriority priority, 
                               String taskType) {
        ThreadPoolWrapper wrapper = threadPools.get(poolType);
        if (wrapper == null) {
            throw new IllegalArgumentException("Unknown pool type: " + poolType);
        }
        
        String taskId = "task_" + wrapper.taskIdGenerator.incrementAndGet() + "_" + 
                       System.currentTimeMillis();
        
        PriorityTask priorityTask = new PriorityTask(() -> {
            long startTime = System.currentTimeMillis();
            wrapper.taskStartTimes.put(taskId, startTime);
            
            try {
                task.call();
            } catch (Exception e) {
                wrapper.stats.recordTaskFailure(e.getClass().getSimpleName());
                throw new RuntimeException(e);
            } finally {
                long executionTime = System.currentTimeMillis() - startTime;
                wrapper.stats.recordTaskCompletion(executionTime);
                wrapper.taskStartTimes.remove(taskId);
                
                if (taskCompletionCallback != null) {
                    TaskResult result = new TaskResult(taskId, true, executionTime, null);
                    taskCompletionCallback.accept(result);
                }
            }
        }, priority, taskId);
        
        wrapper.stats.recordTaskSubmission(taskType != null ? taskType : "default");
        
        try {
            Future<?> future = wrapper.executor.submit(priorityTask);
            wrapper.stats.currentQueueSize.set(wrapper.executor.getQueue().size());
            wrapper.stats.activeThreads.set(wrapper.executor.getActiveCount());
            
            // Can cause ClassCastException at runtime
            @SuppressWarnings("unchecked")
            Future<T> typedFuture = (Future<T>) future;
            return typedFuture;
            
        } catch (RejectedExecutionException e) {
            wrapper.stats.recordTaskRejection();
            throw e;
        }
    }
    
    /**
     * Schedule delayed task
     */
    public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
        String taskId = "scheduled_" + System.currentTimeMillis();
        return scheduledExecutor.schedule(() -> {
            try {
                task.run();
            } catch (Exception e) {
                // Log error
            }
        }, delay, unit);
    }
    
    /**
     * Schedule fixed rate task
     */
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long initialDelay, 
                                                 long period, TimeUnit unit) {
        return scheduledExecutor.scheduleAtFixedRate(task, initialDelay, period, unit);
    }
    
    /**
     * Schedule fixed delay task
     */
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long initialDelay, 
                                                    long delay, TimeUnit unit) {
        return scheduledExecutor.scheduleWithFixedDelay(task, initialDelay, delay, unit);
    }
    
    /**
     * Get thread pool statistics
     */
    public PoolStats getPoolStats(PoolType poolType) {
        ThreadPoolWrapper wrapper = threadPools.get(poolType);
        if (wrapper == null) {
            throw new IllegalArgumentException("Unknown pool type: " + poolType);
        }
        
        wrapper.stats.currentQueueSize.set(wrapper.executor.getQueue().size());
        wrapper.stats.activeThreads.set(wrapper.executor.getActiveCount());
        
        return wrapper.stats;
    }
    
    /**
     * Get all pool statistics
     */
    public Map<PoolType, PoolStats> getAllPoolStats() {
        Map<PoolType, PoolStats> statsMap = new HashMap<>();
        for (PoolType type : threadPools.keySet()) {
            statsMap.put(type, getPoolStats(type));
        }
        return statsMap;
    }
    
    /**
     * Get thread pool configuration
     */
    public PoolConfig getPoolConfig(PoolType poolType) {
        return poolConfigs.get(poolType);
    }
    
    /**
     * Set thread pool configuration
     */
    public void setPoolConfig(PoolType poolType, PoolConfig config) {
        poolLock.lock();
        try {
            poolConfigs.put(poolType, config);
            
            // Recreate thread pool with new config
            ThreadPoolWrapper oldWrapper = threadPools.remove(poolType);
            if (oldWrapper != null) {
                oldWrapper.executor.shutdown();
                try {
                    oldWrapper.executor.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            
            createThreadPool(poolType, config);
        } finally {
            poolLock.unlock();
        }
    }
    
    /**
     * Resize thread pool
     */
    public void resizePool(PoolType poolType, int coreSize, int maxSize) {
        ThreadPoolWrapper wrapper = threadPools.get(poolType);
        if (wrapper == null) {
            throw new IllegalArgumentException("Unknown pool type: " + poolType);
        }
        
        wrapper.executor.setCorePoolSize(coreSize);
        wrapper.executor.setMaximumPoolSize(maxSize);
        
        wrapper.config.corePoolSize = coreSize;
        wrapper.config.maxPoolSize = maxSize;
    }
    
    /**
     * Set task completion callback
     */
    public void setTaskCompletionCallback(Consumer<TaskResult> callback) {
        this.taskCompletionCallback = callback;
    }
    
    /**
     * Get active thread pools
     */
    public Set<PoolType> getActivePools() {
        return new HashSet<>(threadPools.keySet());
    }
    
    /**
     * Get thread pool executor
     */
    public ThreadPoolExecutor getExecutor(PoolType poolType) {
        ThreadPoolWrapper wrapper = threadPools.get(poolType);
        if (wrapper == null) {
            throw new IllegalArgumentException("Unknown pool type: " + poolType);
        }
        
        return wrapper.executor;
    }
    
    /**
     * Shutdown specific pool
     */
    public void shutdownPool(PoolType poolType) {
        ThreadPoolWrapper wrapper = threadPools.remove(poolType);
        if (wrapper != null) {
            wrapper.executor.shutdown();
            try {
                wrapper.executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
    
    /**
     * Shutdown all thread pools
     */
    public void shutdown() {
        shutdown = true;
        
        // Shutdown all thread pools
        for (ThreadPoolWrapper wrapper : threadPools.values()) {
            wrapper.executor.shutdown();
        }
        
        // Shutdown scheduled executor
        scheduledExecutor.shutdown();
        
        try {
            for (ThreadPoolWrapper wrapper : threadPools.values()) {
                wrapper.executor.awaitTermination(5, TimeUnit.SECONDS);
            }
            scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        threadPools.clear();
    }
    
    /**
     * Await termination
     */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long endTime = System.nanoTime() + unit.toNanos(timeout);
        
        for (ThreadPoolWrapper wrapper : threadPools.values()) {
            long remaining = endTime - System.nanoTime();
            if (remaining <= 0) {
                return false;
            }
            if (!wrapper.executor.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
                return false;
            }
        }
        
        long remaining = endTime - System.nanoTime();
        if (remaining <= 0) {
            return false;
        }
        return scheduledExecutor.awaitTermination(remaining, TimeUnit.NANOSECONDS);
    }
    
    /**
     * Check if shutdown
     */
    public boolean isShutdown() {
        return shutdown;
    }
    
    /**
     * Check if terminated
     */
    public boolean isTerminated() {
        for (ThreadPoolWrapper wrapper : threadPools.values()) {
            if (!wrapper.executor.isTerminated()) {
                return false;
            }
        }
        return scheduledExecutor.isTerminated();
    }
}
