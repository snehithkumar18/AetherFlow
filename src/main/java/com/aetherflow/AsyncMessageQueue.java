package com.aetherflow;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Async message queue with thread-safe processing, backpressure handling,
 * and sophisticated queue management for concurrent message processing.
 */
public class AsyncMessageQueue<T> {
    
    // Message wrapper with metadata
    public static class Message<T> {
        public final T payload;
        public final long enqueueTime;
        public final long priority;
        public final String messageId;
        public volatile long dequeueTime;
        public volatile long processStartTime;
        public volatile long processEndTime;
        public volatile boolean processed;
        public volatile boolean failed;
        public volatile String failureReason;
        public volatile int retryCount;
        public final int maxRetries;
        
        public Message(T payload, long priority, int maxRetries) {
            this.payload = payload;
            this.priority = priority;
            this.maxRetries = maxRetries;
            this.enqueueTime = System.currentTimeMillis();
            this.messageId = generateMessageId();
            this.dequeueTime = 0;
            this.processStartTime = 0;
            this.processEndTime = 0;
            this.processed = false;
            this.failed = false;
            this.failureReason = null;
            this.retryCount = 0;
        }
        
        private static String generateMessageId() {
            return "msg_" + System.currentTimeMillis() + "_" + Thread.currentThread().getId();
        }
        
        public long getQueueTime() {
            return dequeueTime > 0 ? dequeueTime - enqueueTime : System.currentTimeMillis() - enqueueTime;
        }
        
        public long getProcessingTime() {
            if (processEndTime > 0 && processStartTime > 0) {
                return processEndTime - processStartTime;
            }
            return 0;
        }
        
        public long getTotalLatency() {
            if (processEndTime > 0) {
                return processEndTime - enqueueTime;
            }
            return 0;
        }
        
        public boolean canRetry() {
            return failed && retryCount < maxRetries;
        }
        
        public void incrementRetry() {
            retryCount++;
        }
    }
    
    // Message processor interface
    public interface MessageProcessor<T> {
        void process(Message<T> message) throws Exception;
    }
    
    // Queue configuration
    private static final int DEFAULT_QUEUE_CAPACITY = 10000;
    private static final int DEFAULT_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final long DEFAULT_PROCESS_TIMEOUT_MS = 30000; // 30 seconds
    private static final int DEFAULT_MAX_RETRIES = 3;
    
    // Queue storage
    private final BlockingQueue<Message<T>> messageQueue;
    private final BlockingQueue<Message<T>> priorityQueue;
    private final ConcurrentHashMap<String, Message<T>> inFlightMessages;
    
    // Thread pool for processing
    private final ThreadPoolExecutor executor;
    
    // Message processor
    private volatile MessageProcessor<T> processor;
    
    // Statistics
    private final AtomicLong totalEnqueued;
    private final AtomicLong totalDequeued;
    private final AtomicLong totalProcessed;
    private final AtomicLong totalFailed;
    private final AtomicLong totalRetried;
    private final AtomicLong totalDropped;
    private final AtomicInteger currentQueueSize;
    private final AtomicInteger currentInFlight;
    
    // Queue state
    private volatile boolean queueEnabled;
    private volatile boolean paused;
    private final AtomicBoolean shutdownRequested;
    
    // Backpressure configuration
    private volatile int backpressureThreshold;
    private volatile boolean backpressureEnabled;
    
    // Lock for queue operations
    private final ReentrantLock queueLock;
    
    // Dead letter queue for failed messages
    private final BlockingQueue<Message<T>> deadLetterQueue;
    
    /**
     * Constructor with default configuration
     */
    public AsyncMessageQueue() {
        this(DEFAULT_QUEUE_CAPACITY, DEFAULT_THREAD_POOL_SIZE);
    }
    
    /**
     * Constructor with custom capacity and thread pool size
     */
    public AsyncMessageQueue(int queueCapacity, int threadPoolSize) {
        this.messageQueue = new LinkedBlockingQueue<>(queueCapacity);
        this.priorityQueue = new LinkedBlockingQueue<>(queueCapacity / 10); // 10% for priority
        this.inFlightMessages = new ConcurrentHashMap<>();
        this.executor = new ThreadPoolExecutor(
            threadPoolSize,
            threadPoolSize * 2,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(queueCapacity)
        );
        this.deadLetterQueue = new LinkedBlockingQueue<>(queueCapacity / 10);
        this.totalEnqueued = new AtomicLong(0);
        this.totalDequeued = new AtomicLong(0);
        this.totalProcessed = new AtomicLong(0);
        this.totalFailed = new AtomicLong(0);
        this.totalRetried = new AtomicLong(0);
        this.totalDropped = new AtomicLong(0);
        this.currentQueueSize = new AtomicInteger(0);
        this.currentInFlight = new AtomicInteger(0);
        this.queueEnabled = true;
        this.paused = false;
        this.shutdownRequested = new AtomicBoolean(false);
        this.backpressureThreshold = (int) (queueCapacity * 0.8);
        this.backpressureEnabled = true;
        this.queueLock = new ReentrantLock();
        
        startProcessingThreads();
    }
    
    /**
     * Set message processor
     */
    public void setProcessor(MessageProcessor<T> processor) {
        this.processor = processor;
    }
    
    /**
     * Enqueue a message
     */
    public boolean enqueue(T payload) {
        return enqueue(payload, 0, DEFAULT_MAX_RETRIES);
    }
    
    /**
     * Enqueue a message with priority and retry configuration
     */
    public boolean enqueue(T payload, long priority, int maxRetries) {
        if (!queueEnabled || paused) {
            totalDropped.incrementAndGet();
            return false;
        }
        
        // Check backpressure
        if (backpressureEnabled && getCurrentQueueSize() >= backpressureThreshold) {
            totalDropped.incrementAndGet();
            return false;
        }
        
        Message<T> message = new Message<>(payload, priority, maxRetries);
        
        queueLock.lock();
        try {
            boolean added;
            if (priority > 0) {
                added = priorityQueue.offer(message);
            } else {
                added = messageQueue.offer(message);
            }
            
            if (added) {
                totalEnqueued.incrementAndGet();
                currentQueueSize.incrementAndGet();
            } else {
                totalDropped.incrementAndGet();
            }
            
            return added;
        } finally {
            queueLock.unlock();
        }
    }
    
    /**
     * Dequeue a message for processing
     */
    private Message<T> dequeue() {
        queueLock.lock();
        try {
            // Try priority queue first
            Message<T> message = priorityQueue.poll();
            if (message != null) {
                message.dequeueTime = System.currentTimeMillis();
                totalDequeued.incrementAndGet();
                currentQueueSize.decrementAndGet();
                currentInFlight.incrementAndGet();
                inFlightMessages.put(message.messageId, message);
                return message;
            }
            
            // Try regular queue
            message = messageQueue.poll();
            if (message != null) {
                message.dequeueTime = System.currentTimeMillis();
                totalDequeued.incrementAndGet();
                currentQueueSize.decrementAndGet();
                currentInFlight.incrementAndGet();
                inFlightMessages.put(message.messageId, message);
            }
            
            return message;
        } finally {
            queueLock.unlock();
        }
    }
    
    /**
     * Process a message
     */
    private void processMessage(Message<T> message) {
        if (processor == null) {
            markFailed(message, "No processor configured");
            return;
        }
        
        message.processStartTime = System.currentTimeMillis();
        
        try {
            processor.process(message);
            markProcessed(message);
        } catch (Exception e) {
            markFailed(message, e.getMessage());
            
            if (message.canRetry()) {
                message.incrementRetry();
                totalRetried.incrementAndGet();
                requeueMessage(message);
            } else {
                sendToDeadLetterQueue(message);
            }
        }
    }
    
    /**
     * Mark message as processed
     */
    private void markProcessed(Message<T> message) {
        message.processEndTime = System.currentTimeMillis();
        message.processed = true;
        message.failed = false;
        
        inFlightMessages.remove(message.messageId);
        currentInFlight.decrementAndGet();
        totalProcessed.incrementAndGet();
    }
    
    /**
     * Mark message as failed
     */
    private void markFailed(Message<T> message, String reason) {
        message.processEndTime = System.currentTimeMillis();
        message.processed = false;
        message.failed = true;
        message.failureReason = reason;
        
        inFlightMessages.remove(message.messageId);
        currentInFlight.decrementAndGet();
        totalFailed.incrementAndGet();
    }
    
    /**
     * Requeue a message for retry
     */
    private void requeueMessage(Message<T> message) {
        queueLock.lock();
        try {
            boolean added = messageQueue.offer(message);
            if (added) {
                currentQueueSize.incrementAndGet();
            } else {
                sendToDeadLetterQueue(message);
            }
        } finally {
            queueLock.unlock();
        }
        
        // The messageQueue is a ConcurrentLinkedQueue which is thread-safe, but the currentQueueSize
        // counter is not atomically updated with the queue operation
        // This can lead to inconsistent state where the queue has messages but size is wrong
    }
    
    /**
     * Send message to dead letter queue
     */
    private void sendToDeadLetterQueue(Message<T> message) {
        deadLetterQueue.offer(message);
    }
    
    /**
     * Start processing threads
     */
    private void startProcessingThreads() {
        int threadCount = executor.getCorePoolSize();
        for (int i = 0; i < threadCount; i++) {
            executor.submit(this::processingLoop);
        }
    }
    
    /**
     * Processing loop for worker threads
     */
    private void processingLoop() {
        while (!shutdownRequested.get()) {
            try {
                if (paused) {
                    Thread.sleep(100);
                    continue;
                }
                
                // The queueLock is held during processing, which can cause priority inversion
                // A low priority message holding the lock can block high priority messages
                queueLock.lock();
                try {
                    Message<T> message = dequeue();
                    if (message != null) {
                        processMessage(message);
                    } else {
                        // No messages available, wait a bit
                        Thread.sleep(10);
                    }
                } finally {
                    queueLock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Continue processing despite errors
            }
        }
    }
    
    /**
     * Get current queue size
     */
    public int getCurrentQueueSize() {
        return currentQueueSize.get();
    }
    
    /**
     * Get current in-flight message count
     */
    public int getCurrentInFlight() {
        return currentInFlight.get();
    }
    
    /**
     * Get queue statistics
     */
    public QueueStats getStats() {
        return new QueueStats(
            totalEnqueued.get(),
            totalDequeued.get(),
            totalProcessed.get(),
            totalFailed.get(),
            totalRetried.get(),
            totalDropped.get(),
            getCurrentQueueSize(),
            getCurrentInFlight(),
            deadLetterQueue.size()
        );
    }
    
    /**
     * Reset statistics
     */
    public void resetStats() {
        totalEnqueued.set(0);
        totalDequeued.set(0);
        totalProcessed.set(0);
        totalFailed.set(0);
        totalRetried.set(0);
        totalDropped.set(0);
    }
    
    /**
     * Pause queue processing
     */
    public void pause() {
        paused = true;
    }
    
    /**
     * Resume queue processing
     */
    public void resume() {
        paused = false;
    }
    
    /**
     * Enable or disable the queue
     */
    public void setEnabled(boolean enabled) {
        this.queueEnabled = enabled;
    }
    
    /**
     * Set backpressure threshold
     */
    public void setBackpressureThreshold(int threshold) {
        this.backpressureThreshold = threshold;
    }
    
    /**
     * Enable or disable backpressure
     */
    public void setBackpressureEnabled(boolean enabled) {
        this.backpressureEnabled = enabled;
    }
    
    /**
     * Shutdown the queue gracefully
     */
    public void shutdown() {
        shutdownRequested.set(true);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Get dead letter queue messages
     */
    public List<Message<T>> getDeadLetterMessages() {
        List<Message<T>> messages = new ArrayList<>();
        deadLetterQueue.drainTo(messages);
        return messages;
    }
    
    /**
     * Clear dead letter queue
     */
    public void clearDeadLetterQueue() {
        deadLetterQueue.clear();
    }
    
    /**
     * Queue statistics
     */
    public static class QueueStats {
        public final long totalEnqueued;
        public final long totalDequeued;
        public final long totalProcessed;
        public final long totalFailed;
        public final long totalRetried;
        public final long totalDropped;
        public final int currentQueueSize;
        public final int currentInFlight;
        public final int deadLetterQueueSize;
        
        public QueueStats(long totalEnqueued, long totalDequeued, long totalProcessed,
                        long totalFailed, long totalRetried, long totalDropped,
                        int currentQueueSize, int currentInFlight, int deadLetterQueueSize) {
            this.totalEnqueued = totalEnqueued;
            this.totalDequeued = totalDequeued;
            this.totalProcessed = totalProcessed;
            this.totalFailed = totalFailed;
            this.totalRetried = totalRetried;
            this.totalDropped = totalDropped;
            this.currentQueueSize = currentQueueSize;
            this.currentInFlight = currentInFlight;
            this.deadLetterQueueSize = deadLetterQueueSize;
        }
    }
}
