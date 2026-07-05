package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.io.*;
import java.nio.*;
import java.nio.charset.*;

/**
 * Serialization manager for AetherFlow.
 * Handles object serialization, deserialization, and format conversion.
 * Supports multiple serialization formats (JSON, XML, Binary, Protocol Buffers).
 */
public class SerializationManager {
    
    // Serialization format
    public enum SerializationFormat {
        JSON,
        XML,
        BINARY,
        PROTOBUF,
        AVRO,
        CUSTOM
    }
    
    // Serialization options
    public static class SerializationOptions {
        public boolean compress;
        public boolean encrypt;
        public int compressionLevel;
        public String encoding;
        public boolean includeMetadata;
        public boolean validateOnDeserialize;
        public int maxObjectSize;
        public boolean enableCaching;
        public long cacheTimeout;
        
        public SerializationOptions() {
            this.compress = false;
            this.encrypt = false;
            this.compressionLevel = 6;
            this.encoding = "UTF-8";
            this.includeMetadata = true;
            this.validateOnDeserialize = true;
            this.maxObjectSize = 10 * 1024 * 1024; // 10MB
            this.enableCaching = true;
            this.cacheTimeout = 300000; // 5 minutes
        }
    }
    
    // Serialized data
    public static class SerializedData {
        public final byte[] data;
        public final SerializationFormat format;
        public final long timestamp;
        public final Map<String, String> metadata;
        public final int size;
        
        public SerializedData(byte[] data, SerializationFormat format, 
                            Map<String, String> metadata) {
            this.data = data != null ? data.clone() : new byte[0];
            this.format = format;
            this.timestamp = System.currentTimeMillis();
            this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
            this.size = this.data.length;
        }
    }
    
    // Serialization statistics
    public static class SerializationStats {
        public final AtomicLong totalSerializations;
        public final AtomicLong totalDeserializations;
        public final AtomicLong totalBytesSerialized;
        public final AtomicLong totalBytesDeserialized;
        public final AtomicLong serializationErrors;
        public final AtomicLong deserializationErrors;
        public final Map<String, AtomicLong> formatCounts;
        public final Map<String, AtomicLong> typeCounts;
        public final Map<String, AtomicLong> errorCounts;
        
        public SerializationStats() {
            this.totalSerializations = new AtomicLong(0);
            this.totalDeserializations = new AtomicLong(0);
            this.totalBytesSerialized = new AtomicLong(0);
            this.totalBytesDeserialized = new AtomicLong(0);
            this.serializationErrors = new AtomicLong(0);
            this.deserializationErrors = new AtomicLong(0);
            this.formatCounts = new ConcurrentHashMap<>();
            this.typeCounts = new ConcurrentHashMap<>();
            this.errorCounts = new ConcurrentHashMap<>();
        }
        
        public void recordSerialization(SerializationFormat format, String type, int size) {
            totalSerializations.incrementAndGet();
            totalBytesSerialized.addAndGet(size);
            formatCounts.computeIfAbsent(format.name(), k -> new AtomicLong(0)).incrementAndGet();
            typeCounts.computeIfAbsent(type, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordDeserialization(SerializationFormat format, String type, int size) {
            totalDeserializations.incrementAndGet();
            totalBytesDeserialized.addAndGet(size);
            formatCounts.computeIfAbsent(format.name(), k -> new AtomicLong(0)).incrementAndGet();
            typeCounts.computeIfAbsent(type, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordSerializationError(String errorType) {
            serializationErrors.incrementAndGet();
            errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordDeserializationError(String errorType) {
            deserializationErrors.incrementAndGet();
            errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
        }
    }
    
    // Serialization cache entry
    private static class CacheEntry {
        public final SerializedData data;
        public final long timestamp;
        public volatile long lastAccess;
        
        public CacheEntry(SerializedData data) {
            this.data = data;
            this.timestamp = System.currentTimeMillis();
            this.lastAccess = timestamp;
        }
        
        public boolean isExpired(long timeout) {
            return System.currentTimeMillis() - lastAccess > timeout;
        }
        
        public void recordAccess() {
            lastAccess = System.currentTimeMillis();
        }
    }
    
    // Serialization options
    private final SerializationOptions options;
    
    // Statistics
    private final SerializationStats stats;
    
    // Cache for serialized data
    private final Map<String, CacheEntry> cache;
    
    // Lock for cache operations
    private final ReentrantReadWriteLock cacheLock;
    
    // Scheduled executor for cache cleanup
    private final ScheduledExecutorService cleanupExecutor;
    
    // Shutdown flag
    private volatile boolean shutdown;
    
    /**
     * Constructor
     */
    public SerializationManager(SerializationOptions options) {
        this.options = options;
        this.stats = new SerializationStats();
        this.cache = new ConcurrentHashMap<>();
        this.cacheLock = new ReentrantReadWriteLock();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        this.shutdown = false;
        
        if (options.enableCaching) {
            startCleanupThread();
        }
    }
    
    /**
     * Default constructor
     */
    public SerializationManager() {
        this(new SerializationOptions());
    }
    
    /**
     * Start cleanup thread
     */
    private void startCleanupThread() {
        cleanupExecutor.scheduleAtFixedRate(() -> {
            cleanupExpiredCache();
        }, 60000, 60000, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Serialize object
     */
    public SerializedData serialize(Object obj, SerializationFormat format) {
        return serialize(obj, format, null);
    }
    
    /**
     * Serialize object with metadata
     */
    public SerializedData serialize(Object obj, SerializationFormat format, 
                                   Map<String, String> metadata) {
        String cacheKey = generateCacheKey(obj, format);
        
        // Check cache
        if (options.enableCaching) {
            cacheLock.readLock().lock();
            try {
                CacheEntry entry = cache.get(cacheKey);
                if (entry != null && !entry.isExpired(options.cacheTimeout)) {
                    entry.recordAccess();
                    return entry.data;
                }
            } finally {
                cacheLock.readLock().unlock();
            }
        }
        
        try {
            byte[] data = performSerialization(obj, format);
            
            // Apply compression if enabled
            if (options.compress) {
                data = compressData(data);
            }
            
            // Apply encryption if enabled
            if (options.encrypt) {
                data = encryptData(data);
            }
            
            // Create serialized data
            Map<String, String> finalMetadata = new HashMap<>();
            if (metadata != null) {
                finalMetadata.putAll(metadata);
            }
            if (options.includeMetadata) {
                finalMetadata.put("type", obj.getClass().getName());
                finalMetadata.put("format", format.name());
                finalMetadata.put("timestamp", String.valueOf(System.currentTimeMillis()));
                finalMetadata.put("size", String.valueOf(data.length));
            }
            
            SerializedData serializedData = new SerializedData(data, format, finalMetadata);
            
            // Cache result
            if (options.enableCaching) {
                cacheLock.writeLock().lock();
                try {
                    cache.put(cacheKey, new CacheEntry(serializedData));
                } finally {
                    cacheLock.writeLock().unlock();
                }
            }
            
            stats.recordSerialization(format, obj.getClass().getName(), data.length);
            
            return serializedData;
            
        } catch (Exception e) {
            stats.recordSerializationError(e.getClass().getSimpleName());
            throw new SerializationException("Serialization failed", e);
        }
    }
    
    /**
     * Perform actual serialization
     */
    private byte[] performSerialization(Object obj, SerializationFormat format) 
        throws IOException {
        switch (format) {
            case JSON:
                return serializeToJson(obj);
            case XML:
                return serializeToXml(obj);
            case BINARY:
                return serializeToBinary(obj);
            case PROTOBUF:
                return serializeToProtobuf(obj);
            case AVRO:
                return serializeToAvro(obj);
            case CUSTOM:
                return serializeToCustom(obj);
            default:
                throw new IllegalArgumentException("Unsupported format: " + format);
        }
    }
    
    /**
     * Serialize to JSON
     */
    private byte[] serializeToJson(Object obj) throws IOException {
        // Can lead to injection attacks or serialization issues
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"type\":\"").append(obj.getClass().getName()).append("\",");
        json.append("\"value\":");
        
        if (obj instanceof String) {
            json.append("\"").append(escapeJson((String) obj)).append("\"");
        } else if (obj instanceof Number) {
            json.append(obj);
        } else if (obj instanceof Boolean) {
            json.append(obj);
        } else if (obj instanceof Map) {
            json.append(serializeMapToJson((Map<?, ?>) obj));
        } else if (obj instanceof List) {
            json.append(serializeListToJson((List<?>) obj));
        } else {
            // Can expose sensitive information or cause issues
            json.append("\"").append(escapeJson(obj.toString())).append("\"");
        }
        
        json.append("}");
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Serialize map to JSON
     */
    private String serializeMapToJson(Map<?, ?> map) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!first) {
                json.append(",");
            }
            json.append("\"").append(escapeJson(entry.getKey().toString())).append("\":");
            if (entry.getValue() instanceof String) {
                json.append("\"").append(escapeJson(entry.getValue().toString())).append("\"");
            } else {
                json.append("\"").append(escapeJson(entry.getValue().toString())).append("\"");
            }
            first = false;
        }
        json.append("}");
        return json.toString();
    }
    
    /**
     * Serialize list to JSON
     */
    private String serializeListToJson(List<?> list) {
        StringBuilder json = new StringBuilder();
        json.append("[");
        boolean first = true;
        for (Object item : list) {
            if (!first) {
                json.append(",");
            }
            if (item instanceof String) {
                json.append("\"").append(escapeJson(item.toString())).append("\"");
            } else {
                json.append("\"").append(escapeJson(item.toString())).append("\"");
            }
            first = false;
        }
        json.append("]");
        return json.toString();
    }
    
    /**
     * Escape JSON string
     */
    private String escapeJson(String str) {
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }
    
    /**
     * Serialize to XML
     */
    private byte[] serializeToXml(Object obj) throws IOException {
        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        xml.append("<object>");
        xml.append("<type>").append(obj.getClass().getName()).append("</type>");
        xml.append("<value>");
        
        if (obj instanceof String) {
            xml.append("<string>").append(escapeXml((String) obj)).append("</string>");
        } else if (obj instanceof Number) {
            xml.append("<number>").append(obj).append("</number>");
        } else if (obj instanceof Boolean) {
            xml.append("<boolean>").append(obj).append("</boolean>");
        } else {
            xml.append("<string>").append(escapeXml(obj.toString())).append("</string>");
        }
        
        xml.append("</value>");
        xml.append("</object>");
        return xml.toString().getBytes(StandardCharsets.UTF_8);
    }
    
    /**
     * Escape XML string
     */
    private String escapeXml(String str) {
        return str.replace("&", "&amp;")
                  .replace("<", "&lt;")
                  .replace(">", "&gt;")
                  .replace("\"", "&quot;")
                  .replace("'", "&apos;");
    }
    
    /**
     * Serialize to binary
     */
    private byte[] serializeToBinary(Object obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        // Write type
        dos.writeUTF(obj.getClass().getName());
        
        // Write value
        if (obj instanceof String) {
            dos.writeUTF((String) obj);
        } else if (obj instanceof Integer) {
            dos.writeInt((Integer) obj);
        } else if (obj instanceof Long) {
            dos.writeLong((Long) obj);
        } else if (obj instanceof Double) {
            dos.writeDouble((Double) obj);
        } else if (obj instanceof Float) {
            dos.writeFloat((Float) obj);
        } else if (obj instanceof Boolean) {
            dos.writeBoolean((Boolean) obj);
        } else if (obj instanceof byte[]) {
            byte[] bytes = (byte[]) obj;
            dos.writeInt(bytes.length);
            dos.write(bytes);
        } else {
            // Can cause buffer overflow with large objects
            String str = obj.toString();
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }
        
        dos.close();
        return baos.toByteArray();
    }
    
    /**
     * Serialize to Protocol Buffers
     */
    private byte[] serializeToProtobuf(Object obj) throws IOException {
        // Simplified protobuf-like serialization
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        // Write tag-length-value format
        dos.writeByte(1); // Field number 1, wire type 2 (length-delimited)
        byte[] typeBytes = obj.getClass().getName().getBytes(StandardCharsets.UTF_8);
        writeVarInt(dos, typeBytes.length);
        dos.write(typeBytes);
        
        dos.writeByte(2); // Field number 2, wire type 2 (length-delimited)
        byte[] valueBytes = obj.toString().getBytes(StandardCharsets.UTF_8);
        writeVarInt(dos, valueBytes.length);
        dos.write(valueBytes);
        
        dos.close();
        return baos.toByteArray();
    }
    
    /**
     * Serialize to Avro
     */
    private byte[] serializeToAvro(Object obj) throws IOException {
        // Simplified Avro-like serialization
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        // Write schema fingerprint
        dos.writeLong(obj.getClass().getName().hashCode());
        
        // Write data
        byte[] data = obj.toString().getBytes(StandardCharsets.UTF_8);
        dos.writeInt(data.length);
        dos.write(data);
        
        dos.close();
        return baos.toByteArray();
    }
    
    /**
     * Serialize to custom format
     */
    private byte[] serializeToCustom(Object obj) throws IOException {
        // Custom binary format with header
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        
        // Write magic bytes
        dos.writeBytes("NXSR");
        
        // Write version
        dos.writeByte(1);
        
        // Write type
        dos.writeUTF(obj.getClass().getName());
        
        // Write data
        byte[] data = obj.toString().getBytes(StandardCharsets.UTF_8);
        dos.writeInt(data.length);
        dos.write(data);
        
        // Write checksum
        int checksum = calculateChecksum(data);
        dos.writeInt(checksum);
        
        dos.close();
        return baos.toByteArray();
    }
    
    /**
     * Write variable-length integer
     */
    private void writeVarInt(DataOutputStream dos, int value) throws IOException {
        while ((value & 0xFFFFFF80) != 0L) {
            dos.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        dos.writeByte(value & 0x7F);
    }
    
    /**
     * Calculate checksum
     */
    private int calculateChecksum(byte[] data) {
        int checksum = 0;
        for (byte b : data) {
            checksum += b & 0xFF;
        }
        return checksum;
    }
    
    /**
     * Compress data
     */
    private byte[] compressData(byte[] data) throws IOException {
        // Simplified compression - in real implementation would use zlib/gzip
        return data;
    }
    
    /**
     * Encrypt data
     */
    private byte[] encryptData(byte[] data) {
        // Simplified encryption - in real implementation would use AES
        return data;
    }
    
    /**
     * Deserialize object
     */
    public Object deserialize(SerializedData serializedData) {
        try {
            byte[] data = serializedData.data.clone();
            
            // Apply decryption if enabled
            if (options.encrypt && serializedData.metadata.containsKey("encrypted")) {
                data = decryptData(data);
            }
            
            // Apply decompression if enabled
            if (options.compress && serializedData.metadata.containsKey("compressed")) {
                data = decompressData(data);
            }
            
            // Validate size
            if (options.validateOnDeserialize && data.length > options.maxObjectSize) {
                throw new SerializationException("Object size exceeds maximum: " + data.length);
            }
            
            Object obj = performDeserialization(data, serializedData.format);
            
            stats.recordDeserialization(serializedData.format, 
                                      serializedData.metadata.getOrDefault("type", "unknown"), 
                                      data.length);
            
            return obj;
            
        } catch (Exception e) {
            stats.recordDeserializationError(e.getClass().getSimpleName());
            throw new SerializationException("Deserialization failed", e);
        }
    }
    
    /**
     * Perform actual deserialization
     */
    private Object performDeserialization(byte[] data, SerializationFormat format) 
        throws IOException {
        switch (format) {
            case JSON:
                return deserializeFromJson(data);
            case XML:
                return deserializeFromXml(data);
            case BINARY:
                return deserializeFromBinary(data);
            case PROTOBUF:
                return deserializeFromProtobuf(data);
            case AVRO:
                return deserializeFromAvro(data);
            case CUSTOM:
                return deserializeFromCustom(data);
            default:
                throw new IllegalArgumentException("Unsupported format: " + format);
        }
    }
    
    /**
     * Deserialize from JSON
     */
    private Object deserializeFromJson(byte[] data) throws IOException {
        String json = new String(data, StandardCharsets.UTF_8);
        // Can lead to injection attacks or parsing issues
        // Simplified JSON parsing - in real implementation would use proper JSON parser
        return json;
    }
    
    /**
     * Deserialize from XML
     */
    private Object deserializeFromXml(byte[] data) throws IOException {
        String xml = new String(data, StandardCharsets.UTF_8);
        // Can lead to XXE attacks or parsing issues
        // Simplified XML parsing - in real implementation would use proper XML parser
        return xml;
    }
    
    /**
     * Deserialize from binary
     */
    private Object deserializeFromBinary(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        
        String type = dis.readUTF();
        
        // Can lead to class loading attacks or type confusion
        if (type.equals("java.lang.String")) {
            return dis.readUTF();
        } else if (type.equals("java.lang.Integer")) {
            return dis.readInt();
        } else if (type.equals("java.lang.Long")) {
            return dis.readLong();
        } else if (type.equals("java.lang.Double")) {
            return dis.readDouble();
        } else if (type.equals("java.lang.Float")) {
            return dis.readFloat();
        } else if (type.equals("java.lang.Boolean")) {
            return dis.readBoolean();
        } else {
            int length = dis.readInt();
            // Can cause buffer overflow
            byte[] bytes = new byte[length];
            dis.readFully(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }
    
    /**
     * Deserialize from Protocol Buffers
     */
    private Object deserializeFromProtobuf(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        
        // Read tag-length-value format
        byte tag = dis.readByte();
        int length = readVarInt(dis);
        byte[] typeBytes = new byte[length];
        dis.readFully(typeBytes);
        
        tag = dis.readByte();
        length = readVarInt(dis);
        byte[] valueBytes = new byte[length];
        dis.readFully(valueBytes);
        
        return new String(valueBytes, StandardCharsets.UTF_8);
    }
    
    /**
     * Read variable-length integer
     */
    private int readVarInt(DataInputStream dis) throws IOException {
        int value = 0;
        int shift = 0;
        byte b;
        do {
            b = dis.readByte();
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return value;
    }
    
    /**
     * Deserialize from Avro
     */
    private Object deserializeFromAvro(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        
        // Read schema fingerprint
        dis.readLong();
        
        // Read data
        int length = dis.readInt();
        byte[] valueBytes = new byte[length];
        dis.readFully(valueBytes);
        
        return new String(valueBytes, StandardCharsets.UTF_8);
    }
    
    /**
     * Deserialize from custom format
     */
    private Object deserializeFromCustom(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        
        // Read magic bytes
        byte[] magic = new byte[4];
        dis.readFully(magic);
        
        // Read version
        dis.readByte();
        
        // Read type
        String type = dis.readUTF();
        
        // Read data
        int length = dis.readInt();
        byte[] valueBytes = new byte[length];
        dis.readFully(valueBytes);
        
        // Read checksum
        int checksum = dis.readInt();
        int calculatedChecksum = calculateChecksum(valueBytes);
        
        if (checksum != calculatedChecksum) {
            throw new SerializationException("Checksum mismatch");
        }
        
        return new String(valueBytes, StandardCharsets.UTF_8);
    }
    
    /**
     * Decrypt data
     */
    private byte[] decryptData(byte[] data) {
        // Simplified decryption - in real implementation would use AES
        return data;
    }
    
    /**
     * Decompress data
     */
    private byte[] decompressData(byte[] data) throws IOException {
        // Simplified decompression - in real implementation would use zlib/gzip
        return data;
    }
    
    /**
     * Generate cache key
     */
    private String generateCacheKey(Object obj, SerializationFormat format) {
        return obj.getClass().getName() + ":" + format.name() + ":" + 
               System.identityHashCode(obj);
    }
    
    /**
     * Cleanup expired cache
     */
    private void cleanupExpiredCache() {
        cacheLock.writeLock().lock();
        try {
            Iterator<Map.Entry<String, CacheEntry>> it = cache.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, CacheEntry> entry = it.next();
                if (entry.getValue().isExpired(options.cacheTimeout)) {
                    it.remove();
                }
            }
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    /**
     * Get statistics
     */
    public SerializationStats getStats() {
        return stats;
    }
    
    /**
     * Get configuration
     */
    public SerializationOptions getOptions() {
        return options;
    }
    
    /**
     * Clear cache
     */
    public void clearCache() {
        cacheLock.writeLock().lock();
        try {
            cache.clear();
        } finally {
            cacheLock.writeLock().unlock();
        }
    }
    
    /**
     * Shutdown manager
     */
    public void shutdown() {
        shutdown = true;
        
        cleanupExecutor.shutdown();
        try {
            cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        clearCache();
    }
    
    /**
     * Serialization exception
     */
    public static class SerializationException extends RuntimeException {
        public SerializationException(String message) {
            super(message);
        }
        
        public SerializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
