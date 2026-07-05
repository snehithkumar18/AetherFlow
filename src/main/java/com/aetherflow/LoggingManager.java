package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.*;

/**
 * Logging manager for AetherFlow.
 * Provides comprehensive logging with multiple levels, formats, and outputs.
 * Supports log rotation, filtering, and async logging.
 */
public class LoggingManager {
    
    // Log level
    public enum LogLevel {
        TRACE(0),
        DEBUG(1),
        INFO(2),
        WARN(3),
        ERROR(4),
        FATAL(5);
        
        private final int level;
        
        LogLevel(int level) {
            this.level = level;
        }
        
        public int getLevel() {
            return level;
        }
        
        public boolean isHigherOrEqual(LogLevel other) {
            return this.level >= other.level;
        }
    }
    
    // Log entry
    public static class LogEntry {
        public final LogLevel level;
        public final String message;
        public final String loggerName;
        public final long timestamp;
        public final String threadName;
        public final StackTraceElement[] stackTrace;
        public final Map<String, Object> context;
        
        public LogEntry(LogLevel level, String message, String loggerName, 
                      String threadName, StackTraceElement[] stackTrace, 
                      Map<String, Object> context) {
            this.level = level;
            this.message = message;
            this.loggerName = loggerName;
            this.timestamp = System.currentTimeMillis();
            this.threadName = threadName;
            this.stackTrace = stackTrace;
            this.context = context != null ? new HashMap<>(context) : new HashMap<>();
        }
        
        public String getFormattedTimestamp() {
            return Instant.ofEpochMilli(timestamp)
                          .atZone(ZoneId.systemDefault())
                          .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
    }
    
    // Log appender
    public interface LogAppender {
        void append(LogEntry entry);
        void flush();
        void close();
    }
    
    // Console appender
    public static class ConsoleAppender implements LogAppender {
        private final PrintStream output;
        
        public ConsoleAppender() {
            this(System.out);
        }
        
        public ConsoleAppender(PrintStream output) {
            this.output = output;
        }
        
        @Override
        public void append(LogEntry entry) {
            String formatted = formatEntry(entry);
            output.println(formatted);
        }
        
        @Override
        public void flush() {
            output.flush();
        }
        
        @Override
        public void close() {
            output.flush();
        }
        
        private String formatEntry(LogEntry entry) {
            return String.format("[%s] [%s] [%s] [%s] %s",
                entry.getFormattedTimestamp(),
                entry.level.name(),
                entry.threadName,
                entry.loggerName,
                entry.message);
        }
    }
    
    // File appender
    public static class FileAppender implements LogAppender {
        protected final String filePath;
        private final boolean append;
        private final int bufferSize;
        protected volatile BufferedWriter writer;
        protected final ReentrantLock writerLock;
        
        public FileAppender(String filePath, boolean append, int bufferSize) throws IOException {
            this.filePath = filePath;
            this.append = append;
            this.bufferSize = bufferSize;
            this.writerLock = new ReentrantLock();
            this.writer = createWriter();
        }
        
        public FileAppender(String filePath) throws IOException {
            this(filePath, true, 8192);
        }
        
        protected BufferedWriter createWriter() throws IOException {
            return new BufferedWriter(new FileWriter(filePath, append), bufferSize);
        }
        
        @Override
        public void append(LogEntry entry) {
            writerLock.lock();
            try {
                if (writer == null) {
                    writer = createWriter();
                }
                
                String formatted = formatEntry(entry);
                writer.write(formatted);
                writer.newLine();
            } catch (IOException e) {
                // Can lead to log loss without notification
            } finally {
                writerLock.unlock();
            }
        }
        
        @Override
        public void flush() {
            writerLock.lock();
            try {
                if (writer != null) {
                    writer.flush();
                }
            } catch (IOException e) {
                // Ignore flush errors
            } finally {
                writerLock.unlock();
            }
        }
        
        @Override
        public void close() {
            writerLock.lock();
            try {
                if (writer != null) {
                    writer.close();
                    writer = null;
                }
            } catch (IOException e) {
                // Ignore close errors
            } finally {
                writerLock.unlock();
            }
        }
        
        private String formatEntry(LogEntry entry) {
            return String.format("[%s] [%s] [%s] [%s] %s",
                entry.getFormattedTimestamp(),
                entry.level.name(),
                entry.threadName,
                entry.loggerName,
                entry.message);
        }
    }
    
    // Rotating file appender
    public static class RotatingFileAppender extends FileAppender {
        private final long maxFileSize;
        private final int maxBackupIndex;
        private volatile long currentFileSize;
        
        public RotatingFileAppender(String filePath, long maxFileSize, int maxBackupIndex) 
            throws IOException {
            super(filePath, true, 8192);
            this.maxFileSize = maxFileSize;
            this.maxBackupIndex = maxBackupIndex;
            this.currentFileSize = getCurrentFileSize();
        }
        
        private long getCurrentFileSize() throws IOException {
            File file = new File(filePath);
            if (file.exists()) {
                return file.length();
            }
            return 0;
        }
        
        @Override
        public void append(LogEntry entry) {
            writerLock.lock();
            try {
                // Check if rotation is needed
                if (currentFileSize > maxFileSize) {
                    rotateFile();
                }
                
                super.append(entry);
                currentFileSize += entry.message.length() + 100; // Approximate size
            } finally {
                writerLock.unlock();
            }
        }
        
        private void rotateFile() {
            try {
                // Close current writer
                close();
                
                // Rotate backup files
                for (int i = maxBackupIndex - 1; i > 0; i--) {
                    File oldFile = new File(filePath + "." + i);
                    File newFile = new File(filePath + "." + (i + 1));
                    if (oldFile.exists()) {
                        if (i == maxBackupIndex - 1) {
                            oldFile.delete();
                        } else {
                            oldFile.renameTo(newFile);
                        }
                    }
                }
                
                // Move current file to .1
                File currentFile = new File(filePath);
                File backupFile = new File(filePath + ".1");
                if (currentFile.exists()) {
                    currentFile.renameTo(backupFile);
                }
                
                // Reset file size
                currentFileSize = 0;
                
                // Create new writer
                writer = createWriter();
                
            } catch (IOException e) {
                // Can lead to disk space exhaustion
            }
        }
    }
    
    // Log filter
    public interface LogFilter {
        boolean accept(LogEntry entry);
    }
    
    // Level filter
    public static class LevelFilter implements LogFilter {
        private final LogLevel minLevel;
        
        public LevelFilter(LogLevel minLevel) {
            this.minLevel = minLevel;
        }
        
        @Override
        public boolean accept(LogEntry entry) {
            return entry.level.isHigherOrEqual(minLevel);
        }
    }
    
    // Logger configuration
    public static class LoggerConfig {
        public LogLevel rootLevel;
        public Map<String, LogLevel> loggerLevels;
        public boolean asyncLogging;
        public int queueSize;
        public int workerThreads;
        public boolean enableContext;
        public boolean enableStackTrace;
        
        public LoggerConfig() {
            this.rootLevel = LogLevel.INFO;
            this.loggerLevels = new ConcurrentHashMap<>();
            this.asyncLogging = true;
            this.queueSize = 10000;
            this.workerThreads = 2;
            this.enableContext = true;
            this.enableStackTrace = true;
        }
    }
    
    // Logger statistics
    public static class LoggerStats {
        public final AtomicLong totalLogs;
        public final Map<String, AtomicLong> levelCounts;
        public final Map<String, AtomicLong> loggerCounts;
        public final AtomicLong droppedLogs;
        public final AtomicLong flushCount;
        
        public LoggerStats() {
            this.totalLogs = new AtomicLong(0);
            this.levelCounts = new ConcurrentHashMap<>();
            this.loggerCounts = new ConcurrentHashMap<>();
            this.droppedLogs = new AtomicLong(0);
            this.flushCount = new AtomicLong(0);
        }
        
        public void recordLog(LogLevel level, String loggerName) {
            totalLogs.incrementAndGet();
            levelCounts.computeIfAbsent(level.name(), k -> new AtomicLong(0)).incrementAndGet();
            loggerCounts.computeIfAbsent(loggerName, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordDroppedLog() {
            droppedLogs.incrementAndGet();
        }
        
        public void recordFlush() {
            flushCount.incrementAndGet();
        }
    }
    
    // Logger instance
    public static class Logger {
        private final String name;
        private final LoggingManager manager;
        
        public Logger(String name, LoggingManager manager) {
            this.name = name;
            this.manager = manager;
        }
        
        public void trace(String message) {
            log(LogLevel.TRACE, message, null);
        }
        
        public void debug(String message) {
            log(LogLevel.DEBUG, message, null);
        }
        
        public void info(String message) {
            log(LogLevel.INFO, message, null);
        }
        
        public void warn(String message) {
            log(LogLevel.WARN, message, null);
        }
        
        public void error(String message) {
            log(LogLevel.ERROR, message, null);
        }
        
        public void fatal(String message) {
            log(LogLevel.FATAL, message, null);
        }
        
        public void trace(String message, Throwable throwable) {
            log(LogLevel.TRACE, message, throwable);
        }
        
        public void debug(String message, Throwable throwable) {
            log(LogLevel.DEBUG, message, throwable);
        }
        
        public void info(String message, Throwable throwable) {
            log(LogLevel.INFO, message, throwable);
        }
        
        public void warn(String message, Throwable throwable) {
            log(LogLevel.WARN, message, throwable);
        }
        
        public void error(String message, Throwable throwable) {
            log(LogLevel.ERROR, message, throwable);
        }
        
        public void fatal(String message, Throwable throwable) {
            log(LogLevel.FATAL, message, throwable);
        }
        
        private void log(LogLevel level, String message, Throwable throwable) {
            StackTraceElement[] stackTrace = null;
            if (manager.config.enableStackTrace && throwable != null) {
                stackTrace = throwable.getStackTrace();
            }
            
            LogEntry entry = new LogEntry(level, message, name, 
                                         Thread.currentThread().getName(), 
                                         stackTrace, null);
            manager.log(entry);
        }
    }
    
    // Logger configuration
    private final LoggerConfig config;
    
    // Log appenders
    private final List<LogAppender> appenders;
    
    // Log filters
    private final List<LogFilter> filters;
    
    // Logger instances
    private final Map<String, Logger> loggers;
    
    // Statistics
    private final LoggerStats stats;
    
    // Lock for appender management
    private final ReentrantLock appenderLock;
    
    // Async logging queue
    private final BlockingQueue<LogEntry> logQueue;
    
    // Worker threads for async logging
    private final ExecutorService workerExecutor;
    
    // Shutdown flag
    private volatile boolean shutdown;
    
    /**
     * Constructor
     */
    public LoggingManager(LoggerConfig config) {
        this.config = config;
        this.appenders = new CopyOnWriteArrayList<>();
        this.filters = new CopyOnWriteArrayList<>();
        this.loggers = new ConcurrentHashMap<>();
        this.stats = new LoggerStats();
        this.appenderLock = new ReentrantLock();
        this.logQueue = new LinkedBlockingQueue<>(config.queueSize);
        this.workerExecutor = Executors.newFixedThreadPool(config.workerThreads);
        this.shutdown = false;
        
        // Start worker threads if async logging is enabled
        if (config.asyncLogging) {
            startWorkerThreads();
        }
        
        // Add default console appender
        try {
            addAppender(new ConsoleAppender());
        } catch (Exception e) {
            // Ignore appender errors
        }
    }
    
    /**
     * Default constructor
     */
    public LoggingManager() {
        this(new LoggerConfig());
    }
    
    /**
     * Start worker threads
     */
    private void startWorkerThreads() {
        for (int i = 0; i < config.workerThreads; i++) {
            workerExecutor.submit(() -> {
                while (!shutdown) {
                    try {
                        LogEntry entry = logQueue.poll(100, TimeUnit.MILLISECONDS);
                        if (entry != null) {
                            processLogEntry(entry);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        // Ignore processing errors
                    }
                }
            });
        }
    }
    
    /**
     * Get logger
     */
    public Logger getLogger(String name) {
        return loggers.computeIfAbsent(name, k -> new Logger(k, this));
    }
    
    /**
     * Log entry
     */
    public void log(LogEntry entry) {
        // Check if logger level is enabled
        LogLevel loggerLevel = config.loggerLevels.getOrDefault(entry.loggerName, config.rootLevel);
        if (!entry.level.isHigherOrEqual(loggerLevel)) {
            return;
        }
        
        // Apply filters
        for (LogFilter filter : filters) {
            if (!filter.accept(entry)) {
                return;
            }
        }
        
        if (config.asyncLogging) {
            boolean added = logQueue.offer(entry);
            if (!added) {
                stats.recordDroppedLog();
            }
        } else {
            processLogEntry(entry);
        }
    }
    
    /**
     * Process log entry
     */
    private void processLogEntry(LogEntry entry) {
        stats.recordLog(entry.level, entry.loggerName);
        
        appenderLock.lock();
        try {
            for (LogAppender appender : appenders) {
                try {
                    appender.append(entry);
                } catch (Exception e) {
                    // Ignore appender errors
                }
            }
        } finally {
            appenderLock.unlock();
        }
    }
    
    /**
     * Add appender
     */
    public void addAppender(LogAppender appender) {
        appenderLock.lock();
        try {
            appenders.add(appender);
        } finally {
            appenderLock.unlock();
        }
    }
    
    /**
     * Remove appender
     */
    public void removeAppender(LogAppender appender) {
        appenderLock.lock();
        try {
            appenders.remove(appender);
        } finally {
            appenderLock.unlock();
        }
    }
    
    /**
     * Add filter
     */
    public void addFilter(LogFilter filter) {
        filters.add(filter);
    }
    
    /**
     * Remove filter
     */
    public void removeFilter(LogFilter filter) {
        filters.remove(filter);
    }
    
    /**
     * Set logger level
     */
    public void setLoggerLevel(String loggerName, LogLevel level) {
        config.loggerLevels.put(loggerName, level);
    }
    
    /**
     * Set root level
     */
    public void setRootLevel(LogLevel level) {
        config.rootLevel = level;
    }
    
    /**
     * Flush all appenders
     */
    public void flush() {
        appenderLock.lock();
        try {
            for (LogAppender appender : appenders) {
                try {
                    appender.flush();
                } catch (Exception e) {
                    // Ignore flush errors
                }
            }
            stats.recordFlush();
        } finally {
            appenderLock.unlock();
        }
    }
    
    /**
     * Get statistics
     */
    public LoggerStats getStats() {
        return stats;
    }
    
    /**
     * Get configuration
     */
    public LoggerConfig getConfig() {
        return config;
    }
    
    /**
     * Get all loggers
     */
    public Collection<Logger> getLoggers() {
        return new ArrayList<>(loggers.values());
    }
    
    /**
     * Shutdown logging manager
     */
    public void shutdown() {
        shutdown = true;
        
        // Process remaining log entries
        while (!logQueue.isEmpty()) {
            LogEntry entry = logQueue.poll();
            if (entry != null) {
                processLogEntry(entry);
            }
        }
        
        // Flush all appenders
        flush();
        
        // Close all appenders
        appenderLock.lock();
        try {
            for (LogAppender appender : appenders) {
                try {
                    appender.close();
                } catch (Exception e) {
                    // Ignore close errors
                }
            }
            appenders.clear();
        } finally {
            appenderLock.unlock();
        }
        
        // Shutdown worker executor
        workerExecutor.shutdown();
        try {
            workerExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        loggers.clear();
        filters.clear();
    }
}
