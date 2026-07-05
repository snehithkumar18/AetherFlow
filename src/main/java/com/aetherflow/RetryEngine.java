package com.aetherflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Retry engine with exponential backoff, circuit breakers, and sophisticated retry logic.
 * Provides configurable retry strategies for resilient operations.
 */
public class RetryEngine {
    
    // Retry policy
    public static class RetryPolicy {
        public final int maxRetries;
        public final long initialBackoffMs;
        public final long maxBackoffMs;
        public final double backoffMultiplier;
        public final boolean jitterEnabled;
        public final double jitterFactor;
        
        public RetryPolicy(int maxRetries, long initialBackoffMs, long maxBackoffMs,
                         double backoffMultiplier, boolean jitterEnabled, double jitterFactor) {
            this.maxRetries = maxRetries;
            this.initialBackoffMs = initialBackoffMs;
            this.maxBackoffMs = maxBackoffMs;
            this.backoffMultiplier = backoffMultiplier;
            this.jitterEnabled = jitterEnabled;
            this.jitterFactor = jitterFactor;
        }
        
        public static RetryPolicy defaultPolicy() {
            return new RetryPolicy(3, 1000, 30000, 2.0, true, 0.1);
        }
        
        public static RetryPolicy aggressivePolicy() {
            return new RetryPolicy(5, 500, 60000, 2.0, true, 0.2);
        }
        
        public static RetryPolicy conservativePolicy() {
            return new RetryPolicy(2, 2000, 10000, 1.5, false, 0.0);
        }
    }
    
    // Circuit breaker state
    public enum CircuitState {
        CLOSED,
        OPEN,
        HALF_OPEN
    }
    
    // Circuit breaker configuration
    public static class CircuitBreakerConfig {
        public final int failureThreshold;
        public final long timeoutMs;
        public final int successThreshold;
        
        public CircuitBreakerConfig(int failureThreshold, long timeoutMs, int successThreshold) {
            this.failureThreshold = failureThreshold;
            this.timeoutMs = timeoutMs;
            this.successThreshold = successThreshold;
        }
        
        public static CircuitBreakerConfig defaultConfig() {
            return new CircuitBreakerConfig(5, 60000, 2);
        }
    }
    
    // Circuit breaker instance
    public static class CircuitBreaker {
        public final String name;
        public volatile CircuitState state;
        public volatile int failureCount;
        public volatile int successCount;
        public volatile long lastFailureTime;
        public volatile long lastStateChangeTime;
        public final CircuitBreakerConfig config;
        
        public CircuitBreaker(String name, CircuitBreakerConfig config) {
            this.name = name;
            this.state = CircuitState.CLOSED;
            this.failureCount = 0;
            this.successCount = 0;
            this.lastFailureTime = 0;
            this.lastStateChangeTime = System.currentTimeMillis();
            this.config = config;
        }
        
        public synchronized void recordFailure() {
            failureCount++;
            lastFailureTime = System.currentTimeMillis();
            
            if (failureCount >= config.failureThreshold && state == CircuitState.CLOSED) {
                transitionTo(CircuitState.OPEN);
            }
        }
        
        public synchronized void recordSuccess() {
            successCount++;
            
            if (state == CircuitState.HALF_OPEN) {
                if (successCount >= config.successThreshold) {
                    transitionTo(CircuitState.CLOSED);
                }
            }
        }
        
        public synchronized void transitionTo(CircuitState newState) {
            state = newState;
            lastStateChangeTime = System.currentTimeMillis();
            
            if (newState == CircuitState.CLOSED) {
                failureCount = 0;
                successCount = 0;
            } else if (newState == CircuitState.HALF_OPEN) {
                successCount = 0;
            }
        }
        
        public synchronized boolean allowRequest() {
            if (state == CircuitState.CLOSED) {
                return true;
            }
            
            if (state == CircuitState.OPEN) {
                long timeSinceFailure = System.currentTimeMillis() - lastFailureTime;
                if (timeSinceFailure >= config.timeoutMs) {
                    transitionTo(CircuitState.HALF_OPEN);
                    return true;
                }
                return false;
            }
            
            if (state == CircuitState.HALF_OPEN) {
                return true;
            }
            
            return false;
        }
        
        public synchronized void reset() {
            transitionTo(CircuitState.CLOSED);
        }
    }
    
    // Retry attempt record
    public static class RetryAttempt {
        public final int attemptNumber;
        public final long timestamp;
        public final long backoffMs;
        public final boolean success;
        public final String error;
        
        public RetryAttempt(int attemptNumber, long backoffMs, boolean success, String error) {
            this.attemptNumber = attemptNumber;
            this.timestamp = System.currentTimeMillis();
            this.backoffMs = backoffMs;
            this.success = success;
            this.error = error;
        }
    }
    
    // Retry operation interface
    public interface RetryOperation<T> {
        T execute() throws Exception;
    }
    
    // Default retry policy
    private volatile RetryPolicy defaultPolicy;
    
    // Circuit breakers by name
    private final Map<String, CircuitBreaker> circuitBreakers;
    
    // Retry history
    private final Map<String, List<RetryAttempt>> retryHistory;
    
    // Statistics
    private final Map<String, AtomicInteger> retryCount;
    private final Map<String, AtomicInteger> successCount;
    private final Map<String, AtomicInteger> failureCount;
    private final Map<String, AtomicLong> totalRetryTime;
    
    // Lock for circuit breaker operations
    private final ReentrantLock circuitLock;
    
    /**
     * Constructor
     */
    public RetryEngine() {
        this.defaultPolicy = RetryPolicy.defaultPolicy();
        this.circuitBreakers = new ConcurrentHashMap<>();
        this.retryHistory = new ConcurrentHashMap<>();
        this.retryCount = new ConcurrentHashMap<>();
        this.successCount = new ConcurrentHashMap<>();
        this.failureCount = new ConcurrentHashMap<>();
        this.totalRetryTime = new ConcurrentHashMap<>();
        this.circuitLock = new ReentrantLock();
    }
    
    /**
     * Set default retry policy
     */
    public void setDefaultPolicy(RetryPolicy policy) {
        this.defaultPolicy = policy;
    }
    
    /**
     * Execute operation with retry
     */
    public <T> T executeWithRetry(String operationName, RetryOperation<T> operation) throws Exception {
        return executeWithRetry(operationName, operation, defaultPolicy);
    }
    
    /**
     * Execute operation with custom retry policy
     */
    public <T> T executeWithRetry(String operationName, RetryOperation<T> operation, RetryPolicy policy) throws Exception {
        // Check circuit breaker
        CircuitBreaker breaker = circuitBreakers.get(operationName);
        if (breaker != null && !breaker.allowRequest()) {
            throw new Exception("Circuit breaker is OPEN for operation: " + operationName);
        }
        
        List<RetryAttempt> attempts = new ArrayList<>();
        int attemptNumber = 0;
        Exception lastException = null;
        long totalStartTime = System.currentTimeMillis();
        
        while (attemptNumber <= policy.maxRetries) {
            attemptNumber++;
            
            try {
                T result = operation.execute();
                
                // Record success
                recordSuccess(operationName, attemptNumber, 0, true, null);
                attempts.add(new RetryAttempt(attemptNumber, 0, true, null));
                
                // Update circuit breaker
                if (breaker != null) {
                    breaker.recordSuccess();
                }
                
                // Store retry history
                retryHistory.put(operationName, attempts);
                
                return result;
                
            } catch (Exception e) {
                lastException = e;
                
                // Record failure
                long backoff = calculateBackoff(attemptNumber, policy);
                recordFailure(operationName, attemptNumber, backoff, false, e.getMessage());
                attempts.add(new RetryAttempt(attemptNumber, backoff, false, e.getMessage()));
                
                // Update circuit breaker
                if (breaker != null) {
                    breaker.recordFailure();
                }
                
                // Check if we should retry
                if (attemptNumber <= policy.maxRetries) {
                    if (backoff > 0) {
                        Thread.sleep(backoff);
                    }
                }
            }
        }
        
        // Store retry history
        retryHistory.put(operationName, attempts);
        
        // Calculate total retry time
        long totalRetryTime = System.currentTimeMillis() - totalStartTime;
        this.totalRetryTime.computeIfAbsent(operationName, k -> new AtomicLong(0)).addAndGet(totalRetryTime);
        
        throw lastException;
    }
    
    /**
     * Calculate backoff with exponential backoff and optional jitter
     */
    private long calculateBackoff(int attemptNumber, RetryPolicy policy) {
        if (attemptNumber == 1) {
            return 0;
        }
        
        long backoff = (long) (policy.initialBackoffMs * Math.pow(policy.backoffMultiplier, attemptNumber - 2));
        backoff = Math.min(backoff, policy.maxBackoffMs);
        
        if (policy.jitterEnabled) {
            double jitter = backoff * policy.jitterFactor * (Math.random() * 2 - 1);
            backoff = (long) (backoff + jitter);
            backoff = Math.max(0, backoff);
        }
        
        return backoff;
    }
    
    /**
     * Record success
     */
    private void recordSuccess(String operationName, int attemptNumber, long backoff, boolean success, String error) {
        retryCount.computeIfAbsent(operationName, k -> new AtomicInteger(0)).incrementAndGet();
        successCount.computeIfAbsent(operationName, k -> new AtomicInteger(0)).incrementAndGet();
        
        // Multiple threads can corrupt the circuit breaker state by concurrent modifications
        // This can lead to incorrect circuit breaker behavior
        CircuitBreaker breaker = circuitBreakers.get(operationName);
        if (breaker != null) {
            // This can cause state corruption
            breaker.recordSuccess();
        }
    }
    
    /**
     * Record failure
     */
    private void recordFailure(String operationName, int attemptNumber, long backoff, boolean success, String error) {
        retryCount.computeIfAbsent(operationName, k -> new AtomicInteger(0)).incrementAndGet();
        failureCount.computeIfAbsent(operationName, k -> new AtomicInteger(0)).incrementAndGet();
        
        // Multiple threads can corrupt the retry history by concurrent modifications
        // This can lead to incorrect retry counts and lost retry information
        List<RetryAttempt> history = retryHistory.get(operationName);
        if (history != null) {
            // This can cause ConcurrentModificationException or data corruption
            history.add(new RetryAttempt(attemptNumber, backoff, success, error));
        }
    }
    
    /**
     * Create or get circuit breaker
     */
    public CircuitBreaker getCircuitBreaker(String name) {
        return circuitBreakers.computeIfAbsent(name, 
            k -> new CircuitBreaker(k, CircuitBreakerConfig.defaultConfig()));
    }
    
    /**
     * Create circuit breaker with custom config
     */
    public CircuitBreaker createCircuitBreaker(String name, CircuitBreakerConfig config) {
        circuitLock.lock();
        try {
            CircuitBreaker breaker = new CircuitBreaker(name, config);
            circuitBreakers.put(name, breaker);
            return breaker;
        } finally {
            circuitLock.unlock();
        }
    }
    
    /**
     * Reset circuit breaker
     */
    public void resetCircuitBreaker(String name) {
        CircuitBreaker breaker = circuitBreakers.get(name);
        if (breaker != null) {
            breaker.reset();
        }
    }
    
    /**
     * Get retry history for operation
     */
    public List<RetryAttempt> getRetryHistory(String operationName) {
        List<RetryAttempt> history = retryHistory.get(operationName);
        if (history == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(history);
    }
    
    /**
     * Get retry statistics
     */
    public RetryStats getStats(String operationName) {
        return new RetryStats(
            retryCount.getOrDefault(operationName, new AtomicInteger(0)).get(),
            successCount.getOrDefault(operationName, new AtomicInteger(0)).get(),
            failureCount.getOrDefault(operationName, new AtomicInteger(0)).get(),
            totalRetryTime.getOrDefault(operationName, new AtomicLong(0)).get()
        );
    }
    
    /**
     * Get all retry statistics
     */
    public Map<String, RetryStats> getAllStats() {
        Map<String, RetryStats> stats = new HashMap<>();
        for (String operationName : retryCount.keySet()) {
            stats.put(operationName, getStats(operationName));
        }
        return stats;
    }
    
    /**
     * Reset statistics for operation
     */
    public void resetStats(String operationName) {
        retryCount.remove(operationName);
        successCount.remove(operationName);
        failureCount.remove(operationName);
        totalRetryTime.remove(operationName);
        retryHistory.remove(operationName);
    }
    
    /**
     * Reset all statistics
     */
    public void resetAllStats() {
        retryCount.clear();
        successCount.clear();
        failureCount.clear();
        totalRetryTime.clear();
        retryHistory.clear();
    }
    
    /**
     * Clear all circuit breakers
     */
    public void clearCircuitBreakers() {
        circuitBreakers.clear();
    }
    
    /**
     * Retry statistics
     */
    public static class RetryStats {
        public final int totalRetries;
        public final int totalSuccesses;
        public final int totalFailures;
        public final long totalRetryTime;
        
        public RetryStats(int totalRetries, int totalSuccesses, int totalFailures, long totalRetryTime) {
            this.totalRetries = totalRetries;
            this.totalSuccesses = totalSuccesses;
            this.totalFailures = totalFailures;
            this.totalRetryTime = totalRetryTime;
        }
        
        public double getSuccessRate() {
            if (totalRetries == 0) {
                return 0.0;
            }
            return (double) totalSuccesses / totalRetries;
        }
        
        public double getAverageRetryTime() {
            if (totalRetries == 0) {
                return 0.0;
            }
            return (double) totalRetryTime / totalRetries;
        }
    }
}
