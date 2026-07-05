package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.function.*;
import java.nio.ByteBuffer;

/**
 * Data stream processor for AetherFlow.
 * Handles streaming data processing, transformation, and aggregation.
 * Supports windowing, filtering, mapping, and complex stream operations.
 */
public class DataStreamProcessor {
    
    // Stream operation types
    public enum StreamOperation {
        MAP,
        FILTER,
        REDUCE,
        AGGREGATE,
        WINDOW,
        JOIN,
        GROUP_BY,
        SORT,
        DISTINCT,
        LIMIT
    }
    
    // Stream window
    public static class StreamWindow {
        public final long size;
        public final long slide;
        public final WindowType type;
        
        public enum WindowType {
            TIME_BASED,
            COUNT_BASED,
            SESSION_BASED
        }
        
        public StreamWindow(long size, long slide, WindowType type) {
            this.size = size;
            this.slide = slide;
            this.type = type;
        }
        
        public static StreamWindow timeBased(long durationMs, long slideMs) {
            return new StreamWindow(durationMs, slideMs, WindowType.TIME_BASED);
        }
        
        public static StreamWindow countBased(long count, long slide) {
            return new StreamWindow(count, slide, WindowType.COUNT_BASED);
        }
    }
    
    // Stream data
    public static class StreamData {
        public final byte[] data;
        public final long timestamp;
        public final Map<String, Object> metadata;
        public final String streamId;
        
        public StreamData(byte[] data, long timestamp, Map<String, Object> metadata, String streamId) {
            this.data = data != null ? data.clone() : new byte[0];
            this.timestamp = timestamp;
            this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
            this.streamId = streamId;
        }
        
        public StreamData(byte[] data, String streamId) {
            this(data, System.currentTimeMillis(), null, streamId);
        }
    }
    
    // Stream result
    public static class StreamResult {
        public final byte[] result;
        public final long processingTime;
        public final int elementCount;
        public final Map<String, Object> statistics;
        
        public StreamResult(byte[] result, long processingTime, int elementCount, 
                          Map<String, Object> statistics) {
            this.result = result != null ? result.clone() : new byte[0];
            this.processingTime = processingTime;
            this.elementCount = elementCount;
            this.statistics = statistics != null ? new HashMap<>(statistics) : new HashMap<>();
        }
    }
    
    // Processor configuration
    public static class ProcessorConfig {
        public int bufferSize;
        public int maxConcurrentStreams;
        public long processingTimeout;
        public boolean enableMetrics;
        public boolean enableCompression;
        public int maxBatchSize;
        public long batchTimeout;
        
        public ProcessorConfig() {
            this.bufferSize = 8192;
            this.maxConcurrentStreams = 100;
            this.processingTimeout = 30000;
            this.enableMetrics = true;
            this.enableCompression = false;
            this.maxBatchSize = 1000;
            this.batchTimeout = 100;
        }
    }
    
    // Processor statistics
    public static class ProcessorStats {
        public final AtomicLong totalProcessed;
        public final AtomicLong totalBytesProcessed;
        public final AtomicLong totalErrors;
        public final AtomicLong totalDropped;
        public final AtomicLong activeStreams;
        public final AtomicLong averageProcessingTime;
        public final Map<String, AtomicLong> operationCounts;
        public final Map<String, AtomicLong> errorCounts;
        
        public ProcessorStats() {
            this.totalProcessed = new AtomicLong(0);
            this.totalBytesProcessed = new AtomicLong(0);
            this.totalErrors = new AtomicLong(0);
            this.totalDropped = new AtomicLong(0);
            this.activeStreams = new AtomicLong(0);
            this.averageProcessingTime = new AtomicLong(0);
            this.operationCounts = new ConcurrentHashMap<>();
            this.errorCounts = new ConcurrentHashMap<>();
        }
        
        public void recordOperation(String operation) {
            operationCounts.computeIfAbsent(operation, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordError(String errorType) {
            errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
        }
    }
    
    // Stream pipeline
    public static class StreamPipeline {
        public final String pipelineId;
        public final List<StreamOperation> operations;
        public final Map<String, Object> parameters;
        public volatile boolean active;
        public final ReentrantLock pipelineLock;
        
        public StreamPipeline(String pipelineId) {
            this.pipelineId = pipelineId;
            this.operations = new ArrayList<>();
            this.parameters = new ConcurrentHashMap<>();
            this.active = true;
            this.pipelineLock = new ReentrantLock();
        }
        
        public void addOperation(StreamOperation operation, Map<String, Object> params) {
            pipelineLock.lock();
            try {
                operations.add(operation);
                if (params != null) {
                    parameters.putAll(params);
                }
            } finally {
                pipelineLock.unlock();
            }
        }
    }
    
    // Processor configuration
    private final ProcessorConfig config;
    
    // Active pipelines
    private final Map<String, StreamPipeline> pipelines;
    
    // Stream data queues
    private final Map<String, BlockingQueue<StreamData>> streamQueues;
    
    // Worker threads
    private final ExecutorService workerExecutor;
    
    // Statistics
    private final ProcessorStats stats;
    
    // Lock for pipeline management
    private final ReentrantLock pipelineLock;
    
    // Shutdown flag
    private volatile boolean shutdown;
    
    /**
     * Constructor
     */
    public DataStreamProcessor(ProcessorConfig config) {
        this.config = config;
        this.pipelines = new ConcurrentHashMap<>();
        this.streamQueues = new ConcurrentHashMap<>();
        this.workerExecutor = Executors.newFixedThreadPool(config.maxConcurrentStreams);
        this.stats = new ProcessorStats();
        this.pipelineLock = new ReentrantLock();
        this.shutdown = false;
    }
    
    /**
     * Default constructor
     */
    public DataStreamProcessor() {
        this(new ProcessorConfig());
    }
    
    /**
     * Create stream pipeline
     */
    public StreamPipeline createPipeline(String pipelineId) {
        pipelineLock.lock();
        try {
            StreamPipeline pipeline = new StreamPipeline(pipelineId);
            pipelines.put(pipelineId, pipeline);
            streamQueues.put(pipelineId, new LinkedBlockingQueue<>(config.bufferSize));
            stats.activeStreams.incrementAndGet();
            
            // Start processing thread for this pipeline
            startPipelineProcessor(pipelineId);
            
            return pipeline;
        } finally {
            pipelineLock.unlock();
        }
    }
    
    /**
     * Start pipeline processor
     */
    private void startPipelineProcessor(String pipelineId) {
        workerExecutor.submit(() -> {
            BlockingQueue<StreamData> queue = streamQueues.get(pipelineId);
            StreamPipeline pipeline = pipelines.get(pipelineId);
            
            while (!shutdown && pipeline.active) {
                try {
                    StreamData data = queue.poll(config.batchTimeout, TimeUnit.MILLISECONDS);
                    if (data != null) {
                        processStreamData(pipelineId, data);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    stats.totalErrors.incrementAndGet();
                    stats.recordError(e.getClass().getSimpleName());
                }
            }
        });
    }
    
    /**
     * Process stream data
     */
    private StreamResult processStreamData(String pipelineId, StreamData data) {
        StreamPipeline pipeline = pipelines.get(pipelineId);
        if (pipeline == null || !pipeline.active) {
            stats.totalDropped.incrementAndGet();
            return null;
        }
        
        long startTime = System.currentTimeMillis();
        byte[] currentData = data.data.clone();
        
        pipeline.pipelineLock.lock();
        try {
            for (StreamOperation operation : pipeline.operations) {
                currentData = applyOperation(operation, currentData, pipeline.parameters);
                stats.recordOperation(operation.name());
            }
            
            long processingTime = System.currentTimeMillis() - startTime;
            stats.totalProcessed.incrementAndGet();
            stats.totalBytesProcessed.addAndGet(currentData.length);
            
            // Update average processing time
            long currentAvg = stats.averageProcessingTime.get();
            long newAvg = (currentAvg * (stats.totalProcessed.get() - 1) + processingTime) / 
                          stats.totalProcessed.get();
            stats.averageProcessingTime.set(newAvg);
            
            Map<String, Object> statistics = new HashMap<>();
            statistics.put("pipelineId", pipelineId);
            statistics.put("operations", pipeline.operations.size());
            statistics.put("processingTime", processingTime);
            
            return new StreamResult(currentData, processingTime, 1, statistics);
            
        } catch (Exception e) {
            stats.totalErrors.incrementAndGet();
            stats.recordError(e.getClass().getSimpleName());
            throw e;
        } finally {
            pipeline.pipelineLock.unlock();
        }
    }
    
    /**
     * Apply stream operation
     */
    private byte[] applyOperation(StreamOperation operation, byte[] data, 
                                  Map<String, Object> parameters) {
        switch (operation) {
            case MAP:
                return applyMapOperation(data, parameters);
            case FILTER:
                return applyFilterOperation(data, parameters);
            case REDUCE:
                return applyReduceOperation(data, parameters);
            case AGGREGATE:
                return applyAggregateOperation(data, parameters);
            case WINDOW:
                return applyWindowOperation(data, parameters);
            case JOIN:
                return applyJoinOperation(data, parameters);
            case GROUP_BY:
                return applyGroupByOperation(data, parameters);
            case SORT:
                return applySortOperation(data, parameters);
            case DISTINCT:
                return applyDistinctOperation(data, parameters);
            case LIMIT:
                return applyLimitOperation(data, parameters);
            default:
                return data;
        }
    }
    
    /**
     * Apply map operation
     */
    private byte[] applyMapOperation(byte[] data, Map<String, Object> parameters) {
        String transformation = (String) parameters.getOrDefault("transform", "identity");
        
        if (transformation.equals("uppercase")) {
            return new String(data).toUpperCase().getBytes();
        } else if (transformation.equals("lowercase")) {
            return new String(data).toLowerCase().getBytes();
        } else if (transformation.equals("reverse")) {
            byte[] reversed = new byte[data.length];
            for (int i = 0; i < data.length; i++) {
                reversed[i] = data[data.length - 1 - i];
            }
            return reversed;
        } else if (transformation.equals("duplicate")) {
            int factor = (int) parameters.getOrDefault("factor", 2);
            byte[] duplicated = new byte[data.length * factor];
            for (int i = 0; i < factor; i++) {
                System.arraycopy(data, 0, duplicated, i * data.length, data.length);
            }
            return duplicated;
        }
        
        return data;
    }
    
    /**
     * Apply filter operation
     */
    private byte[] applyFilterOperation(byte[] data, Map<String, Object> parameters) {
        String pattern = (String) parameters.getOrDefault("pattern", "");
        String dataStr = new String(data);
        
        if (dataStr.contains(pattern)) {
            return data;
        }
        
        return new byte[0];
    }
    
    /**
     * Apply reduce operation
     */
    private byte[] applyReduceOperation(byte[] data, Map<String, Object> parameters) {
        String operation = (String) parameters.getOrDefault("operation", "sum");
        
        if (operation.equals("sum")) {
            int sum = 0;
            for (byte b : data) {
                sum += b;
            }
            return String.valueOf(sum).getBytes();
        } else if (operation.equals("min")) {
            byte min = Byte.MAX_VALUE;
            for (byte b : data) {
                min = (byte) Math.min(min, b);
            }
            return new byte[]{min};
        } else if (operation.equals("max")) {
            byte max = Byte.MIN_VALUE;
            for (byte b : data) {
                max = (byte) Math.max(max, b);
            }
            return new byte[]{max};
        }
        
        return data;
    }
    
    /**
     * Apply aggregate operation
     */
    private byte[] applyAggregateOperation(byte[] data, Map<String, Object> parameters) {
        String aggType = (String) parameters.getOrDefault("type", "count");
        
        if (aggType.equals("count")) {
            return String.valueOf(data.length).getBytes();
        } else if (aggType.equals("avg")) {
            double sum = 0;
            for (byte b : data) {
                sum += b;
            }
            double avg = sum / data.length;
            return String.valueOf(avg).getBytes();
        } else if (aggType.equals("concat")) {
            String separator = (String) parameters.getOrDefault("separator", ",");
            StringBuilder sb = new StringBuilder();
            for (byte b : data) {
                if (sb.length() > 0) {
                    sb.append(separator);
                }
                sb.append(b);
            }
            return sb.toString().getBytes();
        }
        
        return data;
    }
    
    /**
     * Apply window operation
     */
    private byte[] applyWindowOperation(byte[] data, Map<String, Object> parameters) {
        long windowSize = (long) parameters.getOrDefault("windowSize", 1000);
        long windowSlide = (long) parameters.getOrDefault("windowSlide", 500);
        
        // For simplicity, just return the data
        // In a real implementation, this would maintain window state
        return data;
    }
    
    /**
     * Apply join operation
     */
    private byte[] applyJoinOperation(byte[] data, Map<String, Object> parameters) {
        String separator = (String) parameters.getOrDefault("separator", ",");
        byte[] otherData = (byte[]) parameters.get("otherData");
        
        if (otherData != null) {
            byte[] joined = new byte[data.length + separator.length() + otherData.length];
            System.arraycopy(data, 0, joined, 0, data.length);
            System.arraycopy(separator.getBytes(), 0, joined, data.length, separator.length());
            System.arraycopy(otherData, 0, joined, data.length + separator.length(), otherData.length);
            return joined;
        }
        
        return data;
    }
    
    /**
     * Apply group by operation
     */
    private byte[] applyGroupByOperation(byte[] data, Map<String, Object> parameters) {
        String keyFunction = (String) parameters.getOrDefault("keyFunction", "identity");
        
        // For simplicity, just return the data
        // In a real implementation, this would group data by keys
        return data;
    }
    
    /**
     * Apply sort operation
     */
    private byte[] applySortOperation(byte[] data, Map<String, Object> parameters) {
        boolean ascending = (boolean) parameters.getOrDefault("ascending", true);
        
        byte[] sorted = data.clone();
        Arrays.sort(sorted);
        
        if (!ascending) {
            for (int i = 0; i < sorted.length / 2; i++) {
                byte temp = sorted[i];
                sorted[i] = sorted[sorted.length - 1 - i];
                sorted[sorted.length - 1 - i] = temp;
            }
        }
        
        return sorted;
    }
    
    /**
     * Apply distinct operation
     */
    private byte[] applyDistinctOperation(byte[] data, Map<String, Object> parameters) {
        Set<Byte> distinct = new HashSet<>();
        for (byte b : data) {
            distinct.add(b);
        }
        
        byte[] result = new byte[distinct.size()];
        int index = 0;
        for (byte b : distinct) {
            result[index++] = b;
        }
        
        return result;
    }
    
    /**
     * Apply limit operation
     */
    private byte[] applyLimitOperation(byte[] data, Map<String, Object> parameters) {
        int limit = (int) parameters.getOrDefault("limit", data.length);
        
        if (limit >= data.length) {
            return data;
        }
        
        return Arrays.copyOf(data, limit);
    }
    
    /**
     * Submit data to stream
     */
    public boolean submitData(String pipelineId, byte[] data) {
        return submitData(pipelineId, data, null);
    }
    
    /**
     * Submit data to stream with metadata
     */
    public boolean submitData(String pipelineId, byte[] data, Map<String, Object> metadata) {
        BlockingQueue<StreamData> queue = streamQueues.get(pipelineId);
        if (queue == null) {
            stats.totalDropped.incrementAndGet();
            return false;
        }
        
        StreamData streamData = new StreamData(data, System.currentTimeMillis(), 
                                               metadata, pipelineId);
        
        try {
            boolean added = queue.offer(streamData, config.processingTimeout, 
                                      TimeUnit.MILLISECONDS);
            if (!added) {
                stats.totalDropped.incrementAndGet();
            }
            return added;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stats.totalDropped.incrementAndGet();
            return false;
        }
    }
    
    /**
     * Get pipeline
     */
    public StreamPipeline getPipeline(String pipelineId) {
        return pipelines.get(pipelineId);
    }
    
    /**
     * Get all pipelines
     */
    public Collection<StreamPipeline> getPipelines() {
        return new ArrayList<>(pipelines.values());
    }
    
    /**
     * Remove pipeline
     */
    public void removePipeline(String pipelineId) {
        pipelineLock.lock();
        try {
            StreamPipeline pipeline = pipelines.remove(pipelineId);
            if (pipeline != null) {
                pipeline.active = false;
                streamQueues.remove(pipelineId);
                stats.activeStreams.decrementAndGet();
            }
        } finally {
            pipelineLock.unlock();
        }
    }
    
    /**
     * Get statistics
     */
    public ProcessorStats getStats() {
        return stats;
    }
    
    /**
     * Get configuration
     */
    public ProcessorConfig getConfig() {
        return config;
    }
    
    /**
     * Shutdown processor
     */
    public void shutdown() {
        shutdown = true;
        
        // Deactivate all pipelines
        for (StreamPipeline pipeline : pipelines.values()) {
            pipeline.active = false;
        }
        
        workerExecutor.shutdown();
        try {
            workerExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        pipelines.clear();
        streamQueues.clear();
    }
}
