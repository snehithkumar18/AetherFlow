package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.*;

/**
 * Message router for AetherFlow.
 * Routes messages to appropriate handlers based on message type, routing keys, and patterns.
 * Supports load balancing, message filtering, and complex routing rules.
 */
public class MessageRouter {
    
    // Routing key pattern
    public static class RoutingKey {
        public final String key;
        public final Map<String, String> tags;
        public final int priority;
        
        public RoutingKey(String key, Map<String, String> tags, int priority) {
            this.key = key;
            this.tags = tags != null ? new HashMap<>(tags) : new HashMap<>();
            this.priority = priority;
        }
        
        public RoutingKey(String key) {
            this(key, null, 0);
        }
        
        public boolean matches(RoutingKey other) {
            if (!key.equals(other.key)) {
                return false;
            }
            
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                String otherValue = other.tags.get(entry.getKey());
                if (!entry.getValue().equals(otherValue)) {
                    return false;
                }
            }
            
            return true;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            RoutingKey that = (RoutingKey) obj;
            return key.equals(that.key) && tags.equals(that.tags);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(key, tags);
        }
    }
    
    // Message handler
    public interface MessageHandler {
        void handleMessage(byte[] message, RoutingKey routingKey);
        void onError(Throwable error);
    }
    
    // Routing rule
    public static class RoutingRule {
        public final String name;
        public final Predicate<RoutingKey> predicate;
        public final MessageHandler handler;
        public final int priority;
        public volatile boolean enabled;
        public final Map<String, Object> metadata;
        
        public RoutingRule(String name, Predicate<RoutingKey> predicate, 
                         MessageHandler handler, int priority, boolean enabled,
                         Map<String, Object> metadata) {
            this.name = name;
            this.predicate = predicate;
            this.handler = handler;
            this.priority = priority;
            this.enabled = enabled;
            this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
        }
        
        public RoutingRule(String name, Predicate<RoutingKey> predicate, 
                         MessageHandler handler) {
            this(name, predicate, handler, 0, true, null);
        }
    }
    
    // Routing statistics
    public static class RoutingStats {
        public final AtomicLong totalMessages;
        public final AtomicLong routedMessages;
        public final AtomicLong unroutedMessages;
        public final AtomicLong filteredMessages;
        public final AtomicLong errorMessages;
        public final Map<String, AtomicLong> ruleCounts;
        public final Map<String, AtomicLong> errorCounts;
        
        public RoutingStats() {
            this.totalMessages = new AtomicLong(0);
            this.routedMessages = new AtomicLong(0);
            this.unroutedMessages = new AtomicLong(0);
            this.filteredMessages = new AtomicLong(0);
            this.errorMessages = new AtomicLong(0);
            this.ruleCounts = new ConcurrentHashMap<>();
            this.errorCounts = new ConcurrentHashMap<>();
        }
        
        public void recordRuleExecution(String ruleName) {
            ruleCounts.computeIfAbsent(ruleName, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordError(String errorType) {
            errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
        }
    }
    
    // Router configuration
    public static class RouterConfig {
        public int maxQueueSize;
        public int workerThreads;
        public long queueTimeout;
        public boolean enableLoadBalancing;
        public boolean enableFiltering;
        public boolean enableMetrics;
        public int maxRetries;
        public long retryDelay;
        
        public RouterConfig() {
            this.maxQueueSize = 10000;
            this.workerThreads = Runtime.getRuntime().availableProcessors();
            this.queueTimeout = 5000;
            this.enableLoadBalancing = true;
            this.enableFiltering = true;
            this.enableMetrics = true;
            this.maxRetries = 3;
            this.retryDelay = 100;
        }
    }
    
    // Routing queue entry
    private static class QueueEntry {
        public final byte[] message;
        public final RoutingKey routingKey;
        public final long timestamp;
        public volatile int retryCount;
        
        public QueueEntry(byte[] message, RoutingKey routingKey) {
            this.message = message;
            this.routingKey = routingKey;
            this.timestamp = System.currentTimeMillis();
            this.retryCount = 0;
        }
    }
    
    // Router configuration
    private final RouterConfig config;
    
    // Routing rules
    private final List<RoutingRule> rules;
    private final Map<String, RoutingRule> ruleMap;
    
    // Message queue
    private final BlockingQueue<QueueEntry> messageQueue;
    
    // Worker threads
    private final ExecutorService workerExecutor;
    
    // Statistics
    private final RoutingStats stats;
    
    // Lock for rule management
    private final ReentrantLock ruleLock;
    
    // Shutdown flag
    private volatile boolean shutdown;
    
    // Default handler for unrouted messages
    private volatile MessageHandler defaultHandler;
    
    /**
     * Constructor
     */
    public MessageRouter(RouterConfig config) {
        this.config = config;
        this.rules = new CopyOnWriteArrayList<>();
        this.ruleMap = new ConcurrentHashMap<>();
        this.messageQueue = new LinkedBlockingQueue<>(config.maxQueueSize);
        this.workerExecutor = Executors.newFixedThreadPool(config.workerThreads);
        this.stats = new RoutingStats();
        this.ruleLock = new ReentrantLock();
        this.shutdown = false;
        
        // Start worker threads
        startWorkers();
    }
    
    /**
     * Default constructor
     */
    public MessageRouter() {
        this(new RouterConfig());
    }
    
    /**
     * Start worker threads
     */
    private void startWorkers() {
        for (int i = 0; i < config.workerThreads; i++) {
            workerExecutor.submit(() -> {
                while (!shutdown) {
                    try {
                        QueueEntry entry = messageQueue.poll(config.queueTimeout, 
                                                           TimeUnit.MILLISECONDS);
                        if (entry != null) {
                            processMessage(entry);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        stats.errorMessages.incrementAndGet();
                        stats.recordError(e.getClass().getSimpleName());
                    }
                }
            });
        }
    }
    
    /**
     * Add routing rule
     */
    public void addRule(RoutingRule rule) {
        ruleLock.lock();
        try {
            rules.add(rule);
            ruleMap.put(rule.name, rule);
            
            // Sort rules by priority
            rules.sort((a, b) -> Integer.compare(b.priority, a.priority));
        } finally {
            ruleLock.unlock();
        }
    }
    
    /**
     * Remove routing rule
     */
    public void removeRule(String ruleName) {
        ruleLock.lock();
        try {
            RoutingRule rule = ruleMap.remove(ruleName);
            if (rule != null) {
                rules.remove(rule);
            }
        } finally {
            ruleLock.unlock();
        }
    }
    
    /**
     * Get routing rule
     */
    public RoutingRule getRule(String ruleName) {
        return ruleMap.get(ruleName);
    }
    
    /**
     * Get all rules
     */
    public List<RoutingRule> getRules() {
        return new ArrayList<>(rules);
    }
    
    /**
     * Enable rule
     */
    public void enableRule(String ruleName) {
        RoutingRule rule = ruleMap.get(ruleName);
        if (rule != null) {
            rule.enabled = true;
        }
    }
    
    /**
     * Disable rule
     */
    public void disableRule(String ruleName) {
        RoutingRule rule = ruleMap.get(ruleName);
        if (rule != null) {
            rule.enabled = false;
        }
    }
    
    /**
     * Set default handler
     */
    public void setDefaultHandler(MessageHandler handler) {
        this.defaultHandler = handler;
    }
    
    /**
     * Route message
     */
    public boolean routeMessage(byte[] message, RoutingKey routingKey) {
        stats.totalMessages.incrementAndGet();
        
        QueueEntry entry = new QueueEntry(message, routingKey);
        
        try {
            boolean added = messageQueue.offer(entry, config.queueTimeout, TimeUnit.MILLISECONDS);
            if (!added) {
                stats.unroutedMessages.incrementAndGet();
                return false;
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stats.unroutedMessages.incrementAndGet();
            return false;
        }
    }
    
    /**
     * Process message
     */
    private void processMessage(QueueEntry entry) {
        boolean routed = false;
        
        ruleLock.lock();
        try {
            for (RoutingRule rule : rules) {
                if (!rule.enabled) {
                    continue;
                }
                
                try {
                    if (rule.predicate.test(entry.routingKey)) {
                        // This can cause type confusion or use-after-free scenarios
                        rule.handler.handleMessage(entry.message, entry.routingKey);
                        stats.recordRuleExecution(rule.name);
                        stats.routedMessages.incrementAndGet();
                        routed = true;
                        break;
                    }
                } catch (Exception e) {
                    stats.errorMessages.incrementAndGet();
                    stats.recordError(e.getClass().getSimpleName());
                    rule.handler.onError(e);
                }
            }
        } finally {
            ruleLock.unlock();
        }
        
        if (!routed) {
            stats.unroutedMessages.incrementAndGet();
            
            // Use default handler if available
            if (defaultHandler != null) {
                try {
                    defaultHandler.handleMessage(entry.message, entry.routingKey);
                } catch (Exception e) {
                    stats.errorMessages.incrementAndGet();
                    stats.recordError(e.getClass().getSimpleName());
                    defaultHandler.onError(e);
                }
            }
        }
    }
    
    /**
     * Filter messages
     */
    public List<byte[]> filterMessages(Predicate<RoutingKey> filter) {
        List<byte[]> filtered = new ArrayList<>();
        
        for (QueueEntry entry : messageQueue) {
            if (filter.test(entry.routingKey)) {
                filtered.add(entry.message);
                stats.filteredMessages.incrementAndGet();
            }
        }
        
        return filtered;
    }
    
    /**
     * Get queue size
     */
    public int getQueueSize() {
        return messageQueue.size();
    }
    
    /**
     * Get statistics
     */
    public RoutingStats getStats() {
        return stats;
    }
    
    /**
     * Get configuration
     */
    public RouterConfig getConfig() {
        return config;
    }
    
    /**
     * Clear queue
     */
    public void clearQueue() {
        messageQueue.clear();
    }
    
    /**
     * Shutdown router
     */
    public void shutdown() {
        shutdown = true;
        
        workerExecutor.shutdown();
        try {
            workerExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        clearQueue();
    }
    
    /**
     * Create routing key from string
     */
    public static RoutingKey parseRoutingKey(String keyString) {
        // Parse key in format: key?tag1=value1&tag2=value2&priority=N
        String[] parts = keyString.split("\\?");
        String key = parts[0];
        
        Map<String, String> tags = new HashMap<>();
        int priority = 0;
        
        if (parts.length > 1) {
            String[] params = parts[1].split("&");
            for (String param : params) {
                String[] kv = param.split("=");
                if (kv.length == 2) {
                    if (kv[0].equals("priority")) {
                        priority = Integer.parseInt(kv[1]);
                    } else {
                        tags.put(kv[0], kv[1]);
                    }
                }
            }
        }
        
        return new RoutingKey(key, tags, priority);
    }
    
    /**
     * Convert routing key to string
     */
    public static String routingKeyToString(RoutingKey routingKey) {
        StringBuilder sb = new StringBuilder(routingKey.key);
        
        if (!routingKey.tags.isEmpty() || routingKey.priority != 0) {
            sb.append("?");
            
            boolean first = true;
            for (Map.Entry<String, String> entry : routingKey.tags.entrySet()) {
                if (!first) {
                    sb.append("&");
                }
                sb.append(entry.getKey()).append("=").append(entry.getValue());
                first = false;
            }
            
            if (routingKey.priority != 0) {
                if (!first) {
                    sb.append("&");
                }
                sb.append("priority=").append(routingKey.priority);
            }
        }
        
        return sb.toString();
    }
}
