package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.*;

/**
 * Workflow engine for AetherFlow.
 * Manages complex workflows with task dependencies, parallel execution, and state management.
 * Supports workflow orchestration, error handling, and recovery.
 */
public class WorkflowEngine {
    
    // Task state
    public enum TaskState {
        PENDING,
        RUNNING,
        COMPLETED,
        FAILED,
        CANCELLED,
        SKIPPED
    }
    
    // Workflow state
    public enum WorkflowState {
        CREATED,
        RUNNING,
        COMPLETED,
        FAILED,
        CANCELLED,
        PAUSED
    }
    
    // Task definition
    public static class TaskDefinition {
        public final String taskId;
        public final String taskName;
        public final Supplier<TaskResult> taskExecutor;
        public final Set<String> dependencies;
        public final int timeout;
        public final int maxRetries;
        public final Map<String, Object> parameters;
        public volatile TaskState state;
        public volatile TaskResult result;
        public volatile int retryCount;
        public volatile long startTime;
        public volatile long endTime;
        public final ReentrantLock taskLock;
        
        public TaskDefinition(String taskId, String taskName, Supplier<TaskResult> taskExecutor,
                           Set<String> dependencies, int timeout, int maxRetries,
                           Map<String, Object> parameters) {
            this.taskId = taskId;
            this.taskName = taskName;
            this.taskExecutor = taskExecutor;
            this.dependencies = dependencies != null ? new HashSet<>(dependencies) : new HashSet<>();
            this.timeout = timeout;
            this.maxRetries = maxRetries;
            this.parameters = parameters != null ? new HashMap<>(parameters) : new HashMap<>();
            this.state = TaskState.PENDING;
            this.retryCount = 0;
            this.taskLock = new ReentrantLock();
        }
        
        public boolean canExecute(Map<String, TaskDefinition> allTasks) {
            for (String depId : dependencies) {
                TaskDefinition depTask = allTasks.get(depId);
                if (depTask == null || depTask.state != TaskState.COMPLETED) {
                    return false;
                }
            }
            return true;
        }
        
        public long getExecutionTime() {
            if (startTime == 0) return 0;
            long end = endTime != 0 ? endTime : System.currentTimeMillis();
            return end - startTime;
        }
    }
    
    // Task result
    public static class TaskResult {
        public final boolean success;
        public final Object result;
        public final String errorMessage;
        public final Throwable error;
        
        public TaskResult(boolean success, Object result, String errorMessage, Throwable error) {
            this.success = success;
            this.result = result;
            this.errorMessage = errorMessage;
            this.error = error;
        }
        
        public static TaskResult success(Object result) {
            return new TaskResult(true, result, null, null);
        }
        
        public static TaskResult failure(String errorMessage, Throwable error) {
            return new TaskResult(false, null, errorMessage, error);
        }
    }
    
    // Workflow definition
    public static class WorkflowDefinition {
        public final String workflowId;
        public final String workflowName;
        public final List<TaskDefinition> tasks;
        public final Map<String, Object> workflowParameters;
        public volatile WorkflowState state;
        public volatile long creationTime;
        public volatile long startTime;
        public volatile long endTime;
        public final ReentrantLock workflowLock;
        
        public WorkflowDefinition(String workflowId, String workflowName, 
                                List<TaskDefinition> tasks, Map<String, Object> workflowParameters) {
            this.workflowId = workflowId;
            this.workflowName = workflowName;
            this.tasks = tasks != null ? new ArrayList<>(tasks) : new ArrayList<>();
            this.workflowParameters = workflowParameters != null ? new HashMap<>(workflowParameters) : new HashMap<>();
            this.state = WorkflowState.CREATED;
            this.creationTime = System.currentTimeMillis();
            this.workflowLock = new ReentrantLock();
        }
        
        public Map<String, TaskDefinition> getTaskMap() {
            Map<String, TaskDefinition> taskMap = new HashMap<>();
            for (TaskDefinition task : tasks) {
                taskMap.put(task.taskId, task);
            }
            return taskMap;
        }
        
        public List<TaskDefinition> getExecutableTasks() {
            Map<String, TaskDefinition> taskMap = getTaskMap();
            List<TaskDefinition> executable = new ArrayList<>();
            
            for (TaskDefinition task : tasks) {
                if (task.state == TaskState.PENDING && task.canExecute(taskMap)) {
                    executable.add(task);
                }
            }
            
            return executable;
        }
        
        public boolean isComplete() {
            for (TaskDefinition task : tasks) {
                if (task.state != TaskState.COMPLETED && task.state != TaskState.SKIPPED) {
                    return false;
                }
            }
            return true;
        }
        
        public boolean hasFailed() {
            for (TaskDefinition task : tasks) {
                if (task.state == TaskState.FAILED) {
                    return true;
                }
            }
            return false;
        }
        
        public long getExecutionTime() {
            if (startTime == 0) return 0;
            long end = endTime != 0 ? endTime : System.currentTimeMillis();
            return end - startTime;
        }
    }
    
    // Workflow engine configuration
    public static class WorkflowEngineConfig {
        public int maxConcurrentTasks;
        public long taskTimeout;
        public int maxRetries;
        public long retryDelay;
        public boolean enableParallelExecution;
        public boolean enableTaskTimeout;
        public boolean enableWorkflowPersistence;
        public boolean enableMetrics;
        
        public WorkflowEngineConfig() {
            this.maxConcurrentTasks = 10;
            this.taskTimeout = 300000; // 5 minutes
            this.maxRetries = 3;
            this.retryDelay = 1000; // 1 second
            this.enableParallelExecution = true;
            this.enableTaskTimeout = true;
            this.enableWorkflowPersistence = false;
            this.enableMetrics = true;
        }
    }
    
    // Workflow engine statistics
    public static class WorkflowEngineStats {
        public final AtomicLong totalWorkflows;
        public final AtomicLong completedWorkflows;
        public final AtomicLong failedWorkflows;
        public final AtomicLong totalTasks;
        public final AtomicLong completedTasks;
        public final AtomicLong failedTasks;
        public final AtomicLong totalRetries;
        public final Map<String, AtomicLong> taskTypeCounts;
        public final Map<String, AtomicLong> errorCounts;
        
        public WorkflowEngineStats() {
            this.totalWorkflows = new AtomicLong(0);
            this.completedWorkflows = new AtomicLong(0);
            this.failedWorkflows = new AtomicLong(0);
            this.totalTasks = new AtomicLong(0);
            this.completedTasks = new AtomicLong(0);
            this.failedTasks = new AtomicLong(0);
            this.totalRetries = new AtomicLong(0);
            this.taskTypeCounts = new ConcurrentHashMap<>();
            this.errorCounts = new ConcurrentHashMap<>();
        }
        
        public void recordWorkflowStart() {
            totalWorkflows.incrementAndGet();
        }
        
        public void recordWorkflowComplete(boolean success) {
            if (success) {
                completedWorkflows.incrementAndGet();
            } else {
                failedWorkflows.incrementAndGet();
            }
        }
        
        public void recordTaskStart(String taskType) {
            totalTasks.incrementAndGet();
            taskTypeCounts.computeIfAbsent(taskType, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordTaskComplete(boolean success) {
            if (success) {
                completedTasks.incrementAndGet();
            } else {
                failedTasks.incrementAndGet();
            }
        }
        
        public void recordRetry() {
            totalRetries.incrementAndGet();
        }
        
        public void recordError(String errorType) {
            errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
        }
    }
    
    // Workflow engine configuration
    private final WorkflowEngineConfig config;
    
    // Active workflows
    private final Map<String, WorkflowDefinition> activeWorkflows;
    
    // Completed workflows
    private final Map<String, WorkflowDefinition> completedWorkflows;
    
    // Task execution semaphore
    private final Semaphore taskSemaphore;
    
    // Statistics
    private final WorkflowEngineStats stats;
    
    // Lock for workflow management
    private final ReentrantLock workflowLock;
    
    // Executor service for task execution
    private final ExecutorService taskExecutor;
    
    // Shutdown flag
    private volatile boolean shutdown;
    
    // Workflow completion callback
    private volatile Consumer<WorkflowDefinition> workflowCompletionCallback;
    
    /**
     * Constructor
     */
    public WorkflowEngine(WorkflowEngineConfig config) {
        this.config = config;
        this.activeWorkflows = new ConcurrentHashMap<>();
        this.completedWorkflows = new ConcurrentHashMap<>();
        this.taskSemaphore = new Semaphore(config.maxConcurrentTasks);
        this.stats = new WorkflowEngineStats();
        this.workflowLock = new ReentrantLock();
        this.taskExecutor = Executors.newFixedThreadPool(config.maxConcurrentTasks);
        this.shutdown = false;
    }
    
    /**
     * Default constructor
     */
    public WorkflowEngine() {
        this(new WorkflowEngineConfig());
    }
    
    /**
     * Submit workflow
     */
    public void submitWorkflow(WorkflowDefinition workflow) {
        workflowLock.lock();
        try {
            workflow.state = WorkflowState.RUNNING;
            workflow.startTime = System.currentTimeMillis();
            activeWorkflows.put(workflow.workflowId, workflow);
            stats.recordWorkflowStart();
            
            // Start workflow execution
            executeWorkflow(workflow);
            
        } finally {
            workflowLock.unlock();
        }
    }
    
    /**
     * Execute workflow
     */
    private void executeWorkflow(WorkflowDefinition workflow) {
        taskExecutor.submit(() -> {
            while (!shutdown && workflow.state == WorkflowState.RUNNING) {
                workflow.workflowLock.lock();
                try {
                    // Check if workflow is complete
                    if (workflow.isComplete()) {
                        workflow.state = WorkflowState.COMPLETED;
                        workflow.endTime = System.currentTimeMillis();
                        activeWorkflows.remove(workflow.workflowId);
                        completedWorkflows.put(workflow.workflowId, workflow);
                        stats.recordWorkflowComplete(!workflow.hasFailed());
                        
                        if (workflowCompletionCallback != null) {
                            workflowCompletionCallback.accept(workflow);
                        }
                        break;
                    }
                    
                    // Check if workflow has failed
                    if (workflow.hasFailed()) {
                        workflow.state = WorkflowState.FAILED;
                        workflow.endTime = System.currentTimeMillis();
                        activeWorkflows.remove(workflow.workflowId);
                        completedWorkflows.put(workflow.workflowId, workflow);
                        stats.recordWorkflowComplete(false);
                        
                        if (workflowCompletionCallback != null) {
                            workflowCompletionCallback.accept(workflow);
                        }
                        break;
                    }
                    
                    // Get executable tasks
                    List<TaskDefinition> executableTasks = workflow.getExecutableTasks();
                    
                    if (executableTasks.isEmpty()) {
                        // No tasks ready to execute, wait a bit
                        Thread.sleep(100);
                        continue;
                    }
                    
                    // Execute tasks
                    if (config.enableParallelExecution) {
                        // Execute in parallel
                        List<Future<?>> futures = new ArrayList<>();
                        for (TaskDefinition task : executableTasks) {
                            futures.add(taskExecutor.submit(() -> executeTask(workflow, task)));
                        }
                        
                        // Wait for all tasks to complete
                        for (Future<?> future : futures) {
                            try {
                                future.get();
                            } catch (Exception e) {
                                // Task execution error handled in executeTask
                            }
                        }
                    } else {
                        // Execute sequentially
                        for (TaskDefinition task : executableTasks) {
                            executeTask(workflow, task);
                        }
                    }
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    workflow.state = WorkflowState.CANCELLED;
                    break;
                } finally {
                    workflow.workflowLock.unlock();
                }
            }
        });
    }
    
    /**
     * Execute task
     */
    private void executeTask(WorkflowDefinition workflow, TaskDefinition task) {
        task.taskLock.lock();
        try {
            task.state = TaskState.RUNNING;
            task.startTime = System.currentTimeMillis();
            stats.recordTaskStart(task.taskName);
            
            // Acquire semaphore
            taskSemaphore.acquire();
            
            try {
                // Execute task with timeout
                Future<TaskResult> future = taskExecutor.submit(() -> {
                    return task.taskExecutor.get();
                });
                
                TaskResult result;
                if (config.enableTaskTimeout && task.timeout > 0) {
                    result = future.get(task.timeout, TimeUnit.MILLISECONDS);
                } else {
                    result = future.get();
                }
                
                task.result = result;
                
                if (result.success) {
                    task.state = TaskState.COMPLETED;
                    stats.recordTaskComplete(true);
                } else {
                    // Retry logic
                    if (task.retryCount < task.maxRetries) {
                        task.retryCount++;
                        stats.recordRetry();
                        Thread.sleep(config.retryDelay);
                        task.state = TaskState.PENDING;
                    } else {
                        task.state = TaskState.FAILED;
                        stats.recordTaskComplete(false);
                        stats.recordError(result.error != null ? 
                            result.error.getClass().getSimpleName() : "Unknown");
                    }
                }
                
            } catch (TimeoutException e) {
                task.state = TaskState.FAILED;
                task.result = TaskResult.failure("Task timeout", e);
                stats.recordTaskComplete(false);
                stats.recordError("Timeout");
                
            } catch (Exception e) {
                task.state = TaskState.FAILED;
                task.result = TaskResult.failure(e.getMessage(), e);
                stats.recordTaskComplete(false);
                stats.recordError(e.getClass().getSimpleName());
                
            } finally {
                task.endTime = System.currentTimeMillis();
                taskSemaphore.release();
            }
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            task.state = TaskState.CANCELLED;
        } finally {
            task.taskLock.unlock();
        }
    }
    
    /**
     * Cancel workflow
     */
    public void cancelWorkflow(String workflowId) {
        workflowLock.lock();
        try {
            WorkflowDefinition workflow = activeWorkflows.get(workflowId);
            if (workflow != null) {
                workflow.state = WorkflowState.CANCELLED;
                workflow.endTime = System.currentTimeMillis();
                
                // Cancel all running tasks
                for (TaskDefinition task : workflow.tasks) {
                    if (task.state == TaskState.RUNNING) {
                        task.state = TaskState.CANCELLED;
                    }
                }
                
                activeWorkflows.remove(workflowId);
                completedWorkflows.put(workflowId, workflow);
            }
        } finally {
            workflowLock.unlock();
        }
    }
    
    /**
     * Pause workflow
     */
    public void pauseWorkflow(String workflowId) {
        workflowLock.lock();
        try {
            WorkflowDefinition workflow = activeWorkflows.get(workflowId);
            if (workflow != null) {
                workflow.state = WorkflowState.PAUSED;
            }
        } finally {
            workflowLock.unlock();
        }
    }
    
    /**
     * Resume workflow
     */
    public void resumeWorkflow(String workflowId) {
        workflowLock.lock();
        try {
            WorkflowDefinition workflow = activeWorkflows.get(workflowId);
            if (workflow != null && workflow.state == WorkflowState.PAUSED) {
                workflow.state = WorkflowState.RUNNING;
                executeWorkflow(workflow);
            }
        } finally {
            workflowLock.unlock();
        }
    }
    
    /**
     * Get workflow
     */
    public WorkflowDefinition getWorkflow(String workflowId) {
        workflowLock.lock();
        try {
            WorkflowDefinition workflow = activeWorkflows.get(workflowId);
            if (workflow == null) {
                workflow = completedWorkflows.get(workflowId);
            }
            return workflow;
        } finally {
            workflowLock.unlock();
        }
    }
    
    /**
     * Get active workflows
     */
    public Collection<WorkflowDefinition> getActiveWorkflows() {
        workflowLock.lock();
        try {
            return new ArrayList<>(activeWorkflows.values());
        } finally {
            workflowLock.unlock();
        }
    }
    
    /**
     * Get completed workflows
     */
    public Collection<WorkflowDefinition> getCompletedWorkflows() {
        workflowLock.lock();
        try {
            return new ArrayList<>(completedWorkflows.values());
        } finally {
            workflowLock.unlock();
        }
    }
    
    /**
     * Set workflow completion callback
     */
    public void setWorkflowCompletionCallback(Consumer<WorkflowDefinition> callback) {
        this.workflowCompletionCallback = callback;
    }
    
    /**
     * Get statistics
     */
    public WorkflowEngineStats getStats() {
        return stats;
    }
    
    /**
     * Get configuration
     */
    public WorkflowEngineConfig getConfig() {
        return config;
    }
    
    /**
     * Clear completed workflows
     */
    public void clearCompletedWorkflows() {
        workflowLock.lock();
        try {
            completedWorkflows.clear();
        } finally {
            workflowLock.unlock();
        }
    }
    
    /**
     * Shutdown workflow engine
     */
    public void shutdown() {
        shutdown = true;
        
        // Cancel all active workflows
        workflowLock.lock();
        try {
            for (String workflowId : new ArrayList<>(activeWorkflows.keySet())) {
                cancelWorkflow(workflowId);
            }
        } finally {
            workflowLock.unlock();
        }
        
        // Shutdown executor
        taskExecutor.shutdown();
        try {
            taskExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        activeWorkflows.clear();
        completedWorkflows.clear();
    }
}
