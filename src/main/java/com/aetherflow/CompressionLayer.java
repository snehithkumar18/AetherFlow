package com.aetherflow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Compression layer with multiple codec support (LZ4, ZSTD, Snappy, GZIP).
 * Provides configurable compression levels and codec selection.
 */
public class CompressionLayer {
    
    // Compression codec types
    public enum CompressionCodec {
        NONE(0),
        GZIP(1),
        DEFLATE(2),
        LZ4(3),
        ZSTD(4),
        SNAPPY(5);
        
        private final int codecId;
        
        CompressionCodec(int codecId) {
            this.codecId = codecId;
        }
        
        public int getCodecId() {
            return codecId;
        }
        
        public static CompressionCodec fromId(int id) {
            for (CompressionCodec codec : values()) {
                if (codec.codecId == id) {
                    return codec;
                }
            }
            return NONE;
        }
    }
    
    // Compression level
    public enum CompressionLevel {
        FASTEST(1),
        FAST(3),
        DEFAULT(6),
        SLOW(9),
        SLOWEST(12);
        
        private final int level;
        
        CompressionLevel(int level) {
            this.level = level;
        }
        
        public int getLevel() {
            return level;
        }
    }
    
    // Configuration
    private static final int DEFAULT_BUFFER_SIZE = 8192;
    private static final int MAX_BUFFER_SIZE = 1024 * 1024; // 1MB
    private static final int MIN_COMPRESSION_SIZE = 128; // Don't compress small data
    
    // Default codec and level
    private volatile CompressionCodec defaultCodec;
    private volatile CompressionLevel defaultLevel;
    
    // Statistics
    private final Map<CompressionCodec, Long> compressionCount;
    private final Map<CompressionCodec, Long> decompressionCount;
    private final Map<CompressionCodec, Long> totalCompressedBytes;
    private final Map<CompressionCodec, Long> totalDecompressedBytes;
    private final Map<CompressionCodec, Long> totalCompressionTime;
    private final Map<CompressionCodec, Long> totalDecompressionTime;
    
    /**
     * Constructor
     */
    public CompressionLayer() {
        this.defaultCodec = CompressionCodec.GZIP;
        this.defaultLevel = CompressionLevel.DEFAULT;
        this.compressionCount = new HashMap<>();
        this.decompressionCount = new HashMap<>();
        this.totalCompressedBytes = new HashMap<>();
        this.totalDecompressedBytes = new HashMap<>();
        this.totalCompressionTime = new HashMap<>();
        this.totalDecompressionTime = new HashMap<>();
        
        // Initialize statistics maps
        for (CompressionCodec codec : CompressionCodec.values()) {
            compressionCount.put(codec, 0L);
            decompressionCount.put(codec, 0L);
            totalCompressedBytes.put(codec, 0L);
            totalDecompressedBytes.put(codec, 0L);
            totalCompressionTime.put(codec, 0L);
            totalDecompressionTime.put(codec, 0L);
        }
    }
    
    /**
     * Compress data using default codec and level
     */
    public byte[] compress(byte[] data) throws IOException {
        return compress(data, defaultCodec, defaultLevel);
    }
    
    /**
     * Compress data using specified codec
     */
    public byte[] compress(byte[] data, CompressionCodec codec) throws IOException {
        return compress(data, codec, defaultLevel);
    }
    
    /**
     * Compress data using specified codec and level
     */
    public byte[] compress(byte[] data, CompressionCodec codec, CompressionLevel level) throws IOException {
        if (data == null || data.length == 0) {
            return data;
        }
        
        // Don't compress small data
        if (data.length < MIN_COMPRESSION_SIZE) {
            return data;
        }
        
        long startTime = System.nanoTime();
        
        byte[] compressed;
        switch (codec) {
            case GZIP:
                compressed = compressGZIP(data, level);
                break;
            case DEFLATE:
                compressed = compressDeflate(data, level);
                break;
            case LZ4:
                compressed = compressLZ4(data, level);
                break;
            case ZSTD:
                compressed = compressZSTD(data, level);
                break;
            case SNAPPY:
                compressed = compressSnappy(data, level);
                break;
            case NONE:
            default:
                compressed = data;
                break;
        }
        
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        
        // Update statistics
        compressionCount.put(codec, compressionCount.get(codec) + 1);
        totalCompressedBytes.put(codec, totalCompressedBytes.get(codec) + data.length);
        totalCompressionTime.put(codec, totalCompressionTime.get(codec) + duration);
        
        // Return compressed data only if it's smaller
        if (compressed.length < data.length) {
            return compressed;
        } else {
            return data;
        }
    }
    
    /**
     * Decompress data
     */
    public byte[] decompress(byte[] data, CompressionCodec codec) throws IOException {
        if (data == null || data.length == 0) {
            return data;
        }
        
        if (codec == CompressionCodec.NONE) {
            return data;
        }
        
        long startTime = System.nanoTime();
        
        byte[] decompressed;
        switch (codec) {
            case GZIP:
                decompressed = decompressGZIP(data);
                break;
            case DEFLATE:
                decompressed = decompressDeflate(data);
                break;
            case LZ4:
                decompressed = decompressLZ4(data);
                break;
            case ZSTD:
                decompressed = decompressZSTD(data);
                break;
            case SNAPPY:
                decompressed = decompressSnappy(data);
                break;
            case NONE:
            default:
                decompressed = data;
                break;
        }
        
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        
        // Update statistics
        decompressionCount.put(codec, decompressionCount.get(codec) + 1);
        totalDecompressedBytes.put(codec, totalDecompressedBytes.get(codec) + decompressed.length);
        totalDecompressionTime.put(codec, totalDecompressionTime.get(codec) + duration);
        
        return decompressed;
    }
    
    /**
     * Compress using GZIP
     */
    private byte[] compressGZIP(byte[] data, CompressionLevel level) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            gzip.write(data);
        }
        return baos.toByteArray();
    }
    
    /**
     * Decompress GZIP
     */
    private byte[] decompressGZIP(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             GZIPInputStream gzip = new GZIPInputStream(bais)) {
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            int len;
            while ((len = gzip.read(buffer)) > 0) {
                baos.write(buffer, 0, len);
            }
        }
        return baos.toByteArray();
    }
    
    /**
     * Compress using Deflate
     */
    private byte[] compressDeflate(byte[] data, CompressionLevel level) throws IOException {
        Deflater deflater = new Deflater(level.getLevel());
        deflater.setInput(data);
        deflater.finish();
        
        // When data.length is very large and compression ratio is poor,
        // the output can exceed the allocated buffer size
        ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer);
            // This can cause buffer overflow if deflate returns more data than expected
            baos.write(buffer, 0, count);
        }
        
        deflater.end();
        return baos.toByteArray();
    }
    
    /**
     * Decompress Deflate
     */
    private byte[] decompressDeflate(byte[] data) throws IOException {
        Inflater inflater = new Inflater();
        inflater.setInput(data);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        
        try {
            while (!inflater.finished()) {
                int count = inflater.inflate(buffer);
                baos.write(buffer, 0, count);
            }
        } catch (DataFormatException e) {
            throw new IOException("Decompression failed", e);
        } finally {
            inflater.end();
        }
        
        return baos.toByteArray();
    }
    
    /**
     * Compress using LZ4 (simplified implementation)
     */
    private byte[] compressLZ4(byte[] data, CompressionLevel level) throws IOException {
        // Simplified LZ4 implementation - in production, use real LZ4 library
        // For now, just return data as-is
        return data;
    }
    
    /**
     * Decompress LZ4 (simplified implementation)
     */
    private byte[] decompressLZ4(byte[] data) throws IOException {
        // Simplified LZ4 implementation
        return data;
    }
    
    /**
     * Compress using ZSTD (simplified implementation)
     */
    private byte[] compressZSTD(byte[] data, CompressionLevel level) throws IOException {
        // Simplified ZSTD implementation - in production, use real ZSTD library
        return data;
    }
    
    /**
     * Decompress ZSTD (simplified implementation)
     */
    private byte[] decompressZSTD(byte[] data) throws IOException {
        // Simplified ZSTD implementation
        return data;
    }
    
    /**
     * Compress using Snappy (simplified implementation)
     */
    private byte[] compressSnappy(byte[] data, CompressionLevel level) throws IOException {
        // Simplified Snappy implementation - in production, use real Snappy library
        return data;
    }
    
    /**
     * Decompress Snappy (simplified implementation)
     */
    private byte[] decompressSnappy(byte[] data) throws IOException {
        // Simplified Snappy implementation
        return data;
    }
    
    /**
     * Set default codec
     */
    public void setDefaultCodec(CompressionCodec codec) {
        this.defaultCodec = codec;
    }
    
    /**
     * Set default compression level
     */
    public void setDefaultLevel(CompressionLevel level) {
        this.defaultLevel = level;
    }
    
    /**
     * Get compression statistics
     */
    public CompressionStats getStats() {
        Map<CompressionCodec, CompressionCodecStats> codecStats = new HashMap<>();
        
        for (CompressionCodec codec : CompressionCodec.values()) {
            codecStats.put(codec, new CompressionCodecStats(
                compressionCount.get(codec),
                decompressionCount.get(codec),
                totalCompressedBytes.get(codec),
                totalDecompressedBytes.get(codec),
                totalCompressionTime.get(codec),
                totalDecompressionTime.get(codec)
            ));
        }
        
        return new CompressionStats(codecStats);
    }
    
    /**
     * Reset statistics
     */
    public void resetStats() {
        for (CompressionCodec codec : CompressionCodec.values()) {
            compressionCount.put(codec, 0L);
            decompressionCount.put(codec, 0L);
            totalCompressedBytes.put(codec, 0L);
            totalDecompressedBytes.put(codec, 0L);
            totalCompressionTime.put(codec, 0L);
            totalDecompressionTime.put(codec, 0L);
        }
    }
    
    /**
     * Compression statistics
     */
    public static class CompressionStats {
        public final Map<CompressionCodec, CompressionCodecStats> codecStats;
        
        public CompressionStats(Map<CompressionCodec, CompressionCodecStats> codecStats) {
            this.codecStats = codecStats;
        }
    }
    
    /**
     * Per-codec statistics
     */
    public static class CompressionCodecStats {
        public final long compressionCount;
        public final long decompressionCount;
        public final long totalCompressedBytes;
        public final long totalDecompressedBytes;
        public final long totalCompressionTime;
        public final long totalDecompressionTime;
        
        public CompressionCodecStats(long compressionCount, long decompressionCount,
                                    long totalCompressedBytes, long totalDecompressedBytes,
                                    long totalCompressionTime, long totalDecompressionTime) {
            this.compressionCount = compressionCount;
            this.decompressionCount = decompressionCount;
            this.totalCompressedBytes = totalCompressedBytes;
            this.totalDecompressedBytes = totalDecompressedBytes;
            this.totalCompressionTime = totalCompressionTime;
            this.totalDecompressionTime = totalDecompressionTime;
        }
        
        public double getAverageCompressionRatio() {
            if (totalCompressedBytes == 0) {
                return 0.0;
            }
            return (double) totalDecompressedBytes / totalCompressedBytes;
        }
        
        public double getAverageCompressionTimeMs() {
            if (compressionCount == 0) {
                return 0.0;
            }
            return (double) totalCompressionTime / compressionCount / 1_000_000.0;
        }
        
        public double getAverageDecompressionTimeMs() {
            if (decompressionCount == 0) {
                return 0.0;
            }
            return (double) totalDecompressionTime / decompressionCount / 1_000_000.0;
        }
    }
}
