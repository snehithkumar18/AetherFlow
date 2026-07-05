package com.aetherflow;

import java.util.Map;

/**
 * Simulates a telemetry and reporting engine that formats system metrics.
 */
public class ProtocolTelemetryReporter {
    
    private final MetricsCollector metricsCollector;
    private long totalReportsSent;
    
    public ProtocolTelemetryReporter(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
        this.totalReportsSent = 0;
    }
    
    /**
     * Formats the metrics collector summary as a structured log report.
     */
    public synchronized String generateReport() {
        totalReportsSent++;
        Map<String, Object> summary = metricsCollector.getSummary();
        
        StringBuilder report = new StringBuilder();
        report.append("--- AETHERFLOW TELEMETRY REPORT ---\n");
        report.append("Timestamp: ").append(System.currentTimeMillis()).append("\n");
        report.append("Report Sequence: ").append(totalReportsSent).append("\n");
        report.append("Metrics collected:\n");
        
        for (Map.Entry<String, Object> entry : summary.entrySet()) {
            report.append("  ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        
        report.append("------------------------------------\n");
        return report.toString();
    }
    
    public long getTotalReportsSent() {
        return totalReportsSent;
    }
}
