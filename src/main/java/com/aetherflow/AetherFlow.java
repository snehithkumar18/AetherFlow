package com.aetherflow;

import java.util.Map;

/**
 * Main AetherFlow class that ties together all protocol components.
 * Provides a unified interface for protocol operations.
 */
public class AetherFlow {
    
    // Protocol components
    private final ConnectionPool connectionPool;
    private final SessionManager sessionManager;
    private final PacketAssembler packetAssembler;
    private final BufferPool bufferPool;
    private final CompressionLayer compressionLayer;
    private final EncryptionLayer encryptionLayer;
    private final RetryEngine retryEngine;
    private final MetricsCollector metricsCollector;
    private final ProtocolValidator protocolValidator;
    private final AsyncMessageQueue<ProtocolMessage> messageQueue;
    
    // Protocol version
    public static final String VERSION = "2.1.0";
    
    /**
     * Constructor
     */
    public AetherFlow() throws Exception {
        this.connectionPool = new ConnectionPool();
        this.sessionManager = new SessionManager();
        this.packetAssembler = new PacketAssembler();
        this.bufferPool = new BufferPool();
        this.compressionLayer = new CompressionLayer();
        this.encryptionLayer = new EncryptionLayer();
        this.retryEngine = new RetryEngine();
        this.metricsCollector = new MetricsCollector();
        this.protocolValidator = new ProtocolValidator();
        this.messageQueue = new AsyncMessageQueue<>();
        
        // Initialize default metrics
        initializeMetrics();
    }
    
    /**
     * Initialize default metrics
     */
    private void initializeMetrics() {
        metricsCollector.registerCounter("nexus_messages_total", null);
        metricsCollector.registerCounter("nexus_bytes_sent", null);
        metricsCollector.registerCounter("nexus_bytes_received", null);
        metricsCollector.registerGauge("nexus_active_connections", null);
        metricsCollector.registerGauge("nexus_active_sessions", null);
        metricsCollector.registerHistogram("nexus_message_latency_ms", null);
    }
    
    /**
     * Get connection pool
     */
    public ConnectionPool getConnectionPool() {
        return connectionPool;
    }
    
    /**
     * Get session manager
     */
    public SessionManager getSessionManager() {
        return sessionManager;
    }
    
    /**
     * Get packet assembler
     */
    public PacketAssembler getPacketAssembler() {
        return packetAssembler;
    }
    
    /**
     * Get buffer pool
     */
    public BufferPool getBufferPool() {
        return bufferPool;
    }
    
    /**
     * Get compression layer
     */
    public CompressionLayer getCompressionLayer() {
        return compressionLayer;
    }
    
    /**
     * Get encryption layer
     */
    public EncryptionLayer getEncryptionLayer() {
        return encryptionLayer;
    }
    
    /**
     * Get retry engine
     */
    public RetryEngine getRetryEngine() {
        return retryEngine;
    }
    
    /**
     * Get metrics collector
     */
    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }
    
    /**
     * Get protocol validator
     */
    public ProtocolValidator getProtocolValidator() {
        return protocolValidator;
    }
    
    /**
     * Get message queue
     */
    public AsyncMessageQueue<ProtocolMessage> getMessageQueue() {
        return messageQueue;
    }
    
    /**
     * Process a protocol message
     */
    public void processMessage(ProtocolMessage message) throws Exception {
        // Validate message
        ProtocolValidator.ValidationResult result = protocolValidator.validate(message);
        if (!result.valid) {
            throw new Exception("Message validation failed: " + String.join(", ", result.errors));
        }
        
        // Update metrics
        metricsCollector.incrementCounter("nexus_messages_total", null);
        metricsCollector.incrementCounter("nexus_bytes_received", null, message.getPayloadLength());
        
        // Enqueue message for processing
        messageQueue.enqueue(message);
    }
    
    /**
     * Create a protocol message
     */
    public ProtocolMessage createMessage(ProtocolMessage.MessageType type, byte[] payload) {
        ProtocolMessage message = new ProtocolMessage(type, payload);
        message.setChecksum(message.calculateChecksum());
        return message;
    }
    
    /**
     * Serialize a message
     */
    public byte[] serializeMessage(ProtocolMessage message) {
        byte[] serialized = message.serialize();
        
        // Update metrics
        metricsCollector.incrementCounter("nexus_bytes_sent", null, serialized.length);
        
        return serialized;
    }
    
    /**
     * Deserialize a message
     */
    public ProtocolMessage deserializeMessage(byte[] data) {
        return ProtocolMessage.deserialize(data);
    }
    
    /**
     * Get protocol statistics
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new java.util.HashMap<>();
        
        stats.put("version", VERSION);
        stats.put("connectionPool", connectionPool.getStats());
        stats.put("sessionManager", sessionManager.getStats());
        stats.put("packetAssembler", packetAssembler.getStats());
        stats.put("bufferPool", bufferPool.getStats());
        stats.put("compressionLayer", compressionLayer.getStats());
        stats.put("encryptionLayer", encryptionLayer.getStats());
        stats.put("retryEngine", retryEngine.getAllStats());
        stats.put("metrics", metricsCollector.getSummary());
        stats.put("messageQueue", messageQueue.getStats());
        
        return stats;
    }
    
    /**
     * Shutdown the protocol
     */
    public void shutdown() {
        messageQueue.shutdown();
        connectionPool.stopCleanupThread();
        sessionManager.stopCleanupThread();
        encryptionLayer.destroy();
    }
    
    /**
     * Main entry point for testing
     */
    public static void main(String[] args) {
        try {
            AetherFlow protocol = new AetherFlow();
            System.out.println("AetherFlow v" + VERSION + " initialized successfully");
            
            // Create a test message
            ProtocolMessage message = protocol.createMessage(
                ProtocolMessage.MessageType.HANDSHAKE,
                "Hello AetherFlow".getBytes()
            );
            
            System.out.println("Created message: " + message);
            
            // Serialize and deserialize
            byte[] serialized = protocol.serializeMessage(message);
            ProtocolMessage deserialized = protocol.deserializeMessage(serialized);
            
            System.out.println("Deserialized message: " + deserialized);
            
            // Shutdown
            protocol.shutdown();
            
            System.out.println("AetherFlow shutdown complete");
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
