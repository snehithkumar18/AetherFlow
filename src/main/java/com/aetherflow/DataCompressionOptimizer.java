package com.aetherflow;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility to analyze payload characteristics and recommend optimal compression codecs.
 */
public class DataCompressionOptimizer {
    
    private final Map<String, Double> historyRatios;
    private long totalOptimizations;
    
    public DataCompressionOptimizer() {
        this.historyRatios = new HashMap<>();
        this.totalOptimizations = 0;
    }
    
    /**
     * Recommends a compression codec based on payload size and entropy heuristics.
     */
    public CompressionLayer.CompressionCodec optimize(byte[] data) {
        if (data == null || data.length < 128) {
            return CompressionLayer.CompressionCodec.NONE;
        }
        
        totalOptimizations++;
        double entropy = calculateEntropy(data);
        
        // High entropy data (already compressed, encrypted, or media) is not worth compressing
        if (entropy > 7.5) {
            return CompressionLayer.CompressionCodec.NONE;
        }
        
        // Medium entropy large data
        if (data.length > 1024 * 1024) {
            if (entropy < 4.0) {
                return CompressionLayer.CompressionCodec.DEFLATE; // Highly redundant large data
            }
            return CompressionLayer.CompressionCodec.LZ4; // Fast compression for medium entropy large data
        }
        
        // Small redundant data
        if (entropy < 3.0) {
            return CompressionLayer.CompressionCodec.SNAPPY;
        }
        
        return CompressionLayer.CompressionCodec.GZIP; // Balanced default
    }
    
    private double calculateEntropy(byte[] data) {
        int[] frequencies = new int[256];
        for (byte b : data) {
            frequencies[b & 0xFF]++;
        }
        
        double entropy = 0.0;
        double length = data.length;
        for (int freq : frequencies) {
            if (freq > 0) {
                double probability = freq / length;
                entropy -= probability * (Math.log(probability) / Math.log(2));
            }
        }
        return entropy;
    }
    
    public long getTotalOptimizations() {
        return totalOptimizations;
    }
}
