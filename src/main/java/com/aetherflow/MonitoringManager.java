package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.*;

/**
 * Monitoring manager for AetherFlow.
 * Provides system monitoring, metrics collection, and alerting capabilities.
 * Supports custom metrics, thresholds, and notification channels.
 */
public class MonitoringManager {
    
    // Metric type
    public enum MetricType {
        COUNTER,
        GAUGE,
        HISTOGRAM,
        SUMMARY,
        TIMER
    }
    
    // Metric data
    public static class MetricData {
        public final String name;
        public final MetricType type;
        public final double value;
        public final long timestamp;
        public final Map<String, String> tags;
        
        public MetricData(String name, MetricType type, double value, Map<String, String> tags) {
            this.name = name;
            this.type = type;
            this.value = value;
            this.timestamp = System.currentTimeMillis();
            this.tags = tags != null ? new HashMap<>(tags) : new HashMap<>();
        }
    }
    
    // Alert condition
    public static class AlertCondition {
        public final String metricName;
        public final String operator;
        public final double threshold;
        public final int evaluationWindow;
        public final int requiredViolations;
        
        public AlertCondition(String metricName, String operator, double threshold, 
                            int evaluationWindow, int requiredViolations) {
            this.metricName = metricName;
            this.operator = operator;
            this.threshold = threshold;
            this.evaluationWindow = evaluationWindow;
            this.requiredViolations = requiredViolations;
        }
        
        public boolean evaluate(double value) {
            switch (operator) {
                case ">":
                    return value > threshold;
                case "<":
                    return value < threshold;
                case ">=":
                    return value >= threshold;
                case "<=":
                    return value <= threshold;
                case "==":
                    return value == threshold;
                case "!=":
                    return value != threshold;
                default:
                    return false;
            }
        }
    }
    
    // Alert
    public static class Alert {
        public final String alertId;
        public final AlertCondition condition;
        public final double currentValue;
        public final long timestamp;
        public final String message;
        public volatile boolean acknowledged;
        public volatile boolean resolved;
        
        public Alert(String alertId, AlertCondition condition, double currentValue, String message) {
            this.alertId = alertId;
            this.condition = condition;
            this.currentValue = currentValue;
            this.timestamp = System.currentTimeMillis();
            this.message = message;
            this.acknowledged = false;
            this.resolved = false;
        }
    }
    
    // Alert notification handler
    public interface AlertHandler {
        void onAlert(Alert alert);
        void onAlertResolved(String alertId);
    }
    
    // Monitoring configuration
    public static class MonitoringConfig {
        public long collectionInterval;
        public long retentionPeriod;
        public boolean enableAlerts;
        public int maxAlertHistory;
        public boolean enableMetricsExport;
        public String metricsExportFormat;
        public boolean enableHealthChecks;
        public long healthCheckInterval;
        
        public MonitoringConfig() {
            this.collectionInterval = 5000; // 5 seconds
            this.retentionPeriod = 86400000; // 24 hours
            this.enableAlerts = true;
            this.maxAlertHistory = 1000;
            this.enableMetricsExport = true;
            this.metricsExportFormat = "PROMETHEUS";
            this.enableHealthChecks = true;
            this.healthCheckInterval = 30000; // 30 seconds
        }
    }
    
    // Monitoring statistics
    public static class MonitoringStats {
        public final AtomicLong totalMetricsCollected;
        public final AtomicLong totalAlertsTriggered;
        public final AtomicLong totalAlertsResolved;
        public final AtomicLong totalHealthChecks;
        public final Map<String, AtomicLong> metricCounts;
        public final Map<String, AtomicLong> alertCounts;
        
        public MonitoringStats() {
            this.totalMetricsCollected = new AtomicLong(0);
            this.totalAlertsTriggered = new AtomicLong(0);
            this.totalAlertsResolved = new AtomicLong(0);
            this.totalHealthChecks = new AtomicLong(0);
            this.metricCounts = new ConcurrentHashMap<>();
            this.alertCounts = new ConcurrentHashMap<>();
        }
        
        public void recordMetric(String metricName) {
            totalMetricsCollected.incrementAndGet();
            metricCounts.computeIfAbsent(metricName, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordAlert(String alertId) {
            totalAlertsTriggered.incrementAndGet();
            alertCounts.computeIfAbsent(alertId, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordAlertResolved() {
            totalAlertsResolved.incrementAndGet();
        }
        
        public void recordHealthCheck() {
            totalHealthChecks.incrementAndGet();
        }
    }
    
    // Metric registry
    private final Map<String, MetricData> metrics;
    
    // Alert conditions
    private final Map<String, AlertCondition> alertConditions;
    
    // Active alerts
    private final Map<String, Alert> activeAlerts;
    
    // Alert history
    private final List<Alert> alertHistory;
    
    // Alert handlers
    private final List<AlertHandler> alertHandlers;
    
    // Monitoring configuration
    private final MonitoringConfig config;
    
    // Statistics
    private final MonitoringStats stats;
    
    // Lock for metric management
    private final ReentrantReadWriteLock metricLock;
    
    // Scheduled executor for collection
    private final ScheduledExecutorService collectionExecutor;
    
    // Alert ID generator
    private final AtomicLong alertIdGenerator;
    
    // Shutdown flag
    private volatile boolean shutdown;
    
    /**
     * Constructor
     */
    public MonitoringManager(MonitoringConfig config) {
        this.config = config;
        this.metrics = new ConcurrentHashMap<>();
        this.alertConditions = new ConcurrentHashMap<>();
        this.activeAlerts = new ConcurrentHashMap<>();
        this.alertHistory = new CopyOnWriteArrayList<>();
        this.alertHandlers = new CopyOnWriteArrayList<>();
        this.stats = new MonitoringStats();
        this.metricLock = new ReentrantReadWriteLock();
        this.collectionExecutor = Executors.newScheduledThreadPool(4);
        this.alertIdGenerator = new AtomicLong(0);
        this.shutdown = false;
        
        // Start collection thread
        startCollectionThread();
        
        // Start health check thread
        if (config.enableHealthChecks) {
            startHealthCheckThread();
        }
    }
    
    /**
     * Default constructor
     */
    public MonitoringManager() {
        this(new MonitoringConfig());
    }
    
    /**
     * Start collection thread
     */
    private void startCollectionThread() {
        collectionExecutor.scheduleAtFixedRate(() -> {
            collectMetrics();
            evaluateAlerts();
        }, config.collectionInterval, config.collectionInterval, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Start health check thread
     */
    private void startHealthCheckThread() {
        collectionExecutor.scheduleAtFixedRate(() -> {
            performHealthChecks();
        }, config.healthCheckInterval, config.healthCheckInterval, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Record metric
     */
    public void recordMetric(String name, MetricType type, double value) {
        recordMetric(name, type, value, null);
    }
    
    /**
     * Record metric with tags
     */
    public void recordMetric(String name, MetricType type, double value, Map<String, String> tags) {
        metricLock.writeLock().lock();
        try {
            MetricData metric = new MetricData(name, type, value, tags);
            metrics.put(name, metric);
            stats.recordMetric(name);
        } finally {
            metricLock.writeLock().unlock();
        }
    }
    
    /**
     * Get metric
     */
    public MetricData getMetric(String name) {
        metricLock.readLock().lock();
        try {
            return metrics.get(name);
        } finally {
            metricLock.readLock().unlock();
        }
    }
    
    /**
     * Get all metrics
     */
    public Map<String, MetricData> getMetrics() {
        metricLock.readLock().lock();
        try {
            return new HashMap<>(metrics);
        } finally {
            metricLock.readLock().unlock();
        }
    }
    
    /**
     * Add alert condition
     */
    public void addAlertCondition(String conditionId, AlertCondition condition) {
        alertConditions.put(conditionId, condition);
    }
    
    /**
     * Remove alert condition
     */
    public void removeAlertCondition(String conditionId) {
        alertConditions.remove(conditionId);
    }
    
    /**
     * Add alert handler
     */
    public void addAlertHandler(AlertHandler handler) {
        alertHandlers.add(handler);
    }
    
    /**
     * Remove alert handler
     */
    public void removeAlertHandler(AlertHandler handler) {
        alertHandlers.remove(handler);
    }
    
    /**
     * Collect metrics
     */
    private void collectMetrics() {
        // In a real implementation, this would collect system metrics
        // For now, we just record collection time
        recordMetric("collection.time", MetricType.GAUGE, System.currentTimeMillis());
    }
    
    /**
     * Evaluate alerts
     */
    private void evaluateAlerts() {
        if (!config.enableAlerts) {
            return;
        }
        
        for (Map.Entry<String, AlertCondition> entry : alertConditions.entrySet()) {
            String conditionId = entry.getKey();
            AlertCondition condition = entry.getValue();
            
            MetricData metric = getMetric(condition.metricName);
            if (metric == null) {
                continue;
            }
            
            if (condition.evaluate(metric.value)) {
                triggerAlert(conditionId, condition, metric.value);
            } else {
                resolveAlert(conditionId);
            }
        }
    }
    
    /**
     * Trigger alert
     */
    private void triggerAlert(String conditionId, AlertCondition condition, double currentValue) {
        if (activeAlerts.containsKey(conditionId)) {
            return;
        }
        
        String alertId = "alert_" + alertIdGenerator.incrementAndGet() + "_" + 
                       System.currentTimeMillis();
        String message = String.format("Alert triggered: %s %s %f (current: %f)", 
                                      condition.metricName, condition.operator, 
                                      condition.threshold, currentValue);
        
        Alert alert = new Alert(alertId, condition, currentValue, message);
        activeAlerts.put(conditionId, alert);
        alertHistory.add(alert);
        
        // Keep alert history within limits
        while (alertHistory.size() > config.maxAlertHistory) {
            alertHistory.remove(0);
        }
        
        stats.recordAlert(alertId);
        
        // Notify handlers
        for (AlertHandler handler : alertHandlers) {
            handler.onAlert(alert);
        }
    }
    
    /**
     * Resolve alert
     */
    private void resolveAlert(String conditionId) {
        Alert alert = activeAlerts.remove(conditionId);
        if (alert != null) {
            alert.resolved = true;
            stats.recordAlertResolved();
            
            // Notify handlers
            for (AlertHandler handler : alertHandlers) {
                handler.onAlertResolved(alert.alertId);
            }
        }
    }
    
    /**
     * Acknowledge alert
     */
    public void acknowledgeAlert(String alertId) {
        for (Alert alert : alertHistory) {
            if (alert.alertId.equals(alertId)) {
                alert.acknowledged = true;
                break;
            }
        }
    }
    
    /**
     * Perform health checks
     */
    private void performHealthChecks() {
        stats.recordHealthCheck();
        
        // Can lead to undetected system failures
        // In a real implementation, this would perform actual health checks
        // For now, we just record that health checks were performed
        recordMetric("health.check.status", MetricType.GAUGE, 1.0);
    }
    
    /**
     * Get active alerts
     */
    public Collection<Alert> getActiveAlerts() {
        return new ArrayList<>(activeAlerts.values());
    }
    
    /**
     * Get alert history
     */
    public List<Alert> getAlertHistory() {
        return new ArrayList<>(alertHistory);
    }
    
    /**
     * Get statistics
     */
    public MonitoringStats getStats() {
        return stats;
    }
    
    /**
     * Get configuration
     */
    public MonitoringConfig getConfig() {
        return config;
    }
    
    /**
     * Export metrics
     */
    public String exportMetrics() {
        StringBuilder sb = new StringBuilder();
        
        for (MetricData metric : metrics.values()) {
            sb.append(metric.name);
            sb.append(" ");
            sb.append(metric.value);
            
            if (!metric.tags.isEmpty()) {
                for (Map.Entry<String, String> tag : metric.tags.entrySet()) {
                    sb.append(" ");
                    sb.append(tag.getKey());
                    sb.append("=\"");
                    sb.append(tag.getValue());
                    sb.append("\"");
                }
            }
            
            sb.append(" ");
            sb.append(metric.timestamp);
            sb.append("\n");
        }
        
        return sb.toString();
    }
    
    /**
     * Clear old metrics
     */
    public void clearOldMetrics() {
        long cutoff = System.currentTimeMillis() - config.retentionPeriod;
        
        metricLock.writeLock().lock();
        try {
            Iterator<Map.Entry<String, MetricData>> it = metrics.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, MetricData> entry = it.next();
                if (entry.getValue().timestamp < cutoff) {
                    it.remove();
                }
            }
        } finally {
            metricLock.writeLock().unlock();
        }
    }
    
    /**
     * Shutdown monitoring manager
     */
    public void shutdown() {
        shutdown = true;
        
        collectionExecutor.shutdown();
        try {
            collectionExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        metrics.clear();
        alertConditions.clear();
        activeAlerts.clear();
        alertHistory.clear();
        alertHandlers.clear();
    }
}
