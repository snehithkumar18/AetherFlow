package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.io.*;
import java.nio.file.*;
import java.util.function.*;

/**
 * Configuration manager for AetherFlow.
 * Manages application configuration with support for multiple sources, hot reloading, and validation.
 * Supports file-based, environment-based, and programmatic configuration.
 */
public class ConfigurationManager {
    
    // Configuration source type
    public enum ConfigSourceType {
        FILE,
        ENVIRONMENT,
        SYSTEM_PROPERTIES,
        PROGRAMMATIC,
        REMOTE,
        DATABASE
    }
    
    // Configuration entry
    public static class ConfigEntry {
        public final String key;
        public final String value;
        public final ConfigSourceType source;
        public final long lastModified;
        public final String sourceLocation;
        public volatile boolean encrypted;
        public final Map<String, String> metadata;
        
        public ConfigEntry(String key, String value, ConfigSourceType source, 
                          String sourceLocation, Map<String, String> metadata) {
            this.key = key;
            this.value = value;
            this.source = source;
            this.lastModified = System.currentTimeMillis();
            this.sourceLocation = sourceLocation;
            this.encrypted = false;
            this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
        }
        
        public ConfigEntry(String key, String value, ConfigSourceType source) {
            this(key, value, source, null, null);
        }
        
        public <T> T getValueAs(Class<T> type) {
            if (type == String.class) {
                return type.cast(value);
            } else if (type == Integer.class || type == int.class) {
                return type.cast(Integer.parseInt(value));
            } else if (type == Long.class || type == long.class) {
                return type.cast(Long.parseLong(value));
            } else if (type == Double.class || type == double.class) {
                return type.cast(Double.parseDouble(value));
            } else if (type == Boolean.class || type == boolean.class) {
                return type.cast(Boolean.parseBoolean(value));
            } else {
                throw new IllegalArgumentException("Unsupported type: " + type);
            }
        }
    }
    
    // Configuration validator
    public interface ConfigValidator {
        boolean validate(String key, String value);
        String getErrorMessage();
    }
    
    // Configuration change listener
    public interface ConfigChangeListener {
        void onConfigChanged(String key, String oldValue, String newValue);
        void onConfigAdded(String key, String value);
        void onConfigRemoved(String key);
    }
    
    // Configuration manager configuration
    public static class ConfigManagerConfig {
        public boolean enableHotReload;
        public long reloadInterval;
        public boolean enableValidation;
        public boolean enableEncryption;
        public String encryptionKey;
        public boolean enableBackup;
        public int backupCount;
        public boolean enableRemoteSync;
        public String remoteSyncUrl;
        public long remoteSyncInterval;
        public boolean enableCache;
        public long cacheTimeout;
        
        public ConfigManagerConfig() {
            this.enableHotReload = true;
            this.reloadInterval = 60000; // 1 minute
            this.enableValidation = true;
            this.enableEncryption = false;
            this.encryptionKey = null;
            this.enableBackup = true;
            this.backupCount = 5;
            this.enableRemoteSync = false;
            this.remoteSyncUrl = null;
            this.remoteSyncInterval = 300000; // 5 minutes
            this.enableCache = true;
            this.cacheTimeout = 300000; // 5 minutes
        }
    }
    
    // Configuration statistics
    public static class ConfigStats {
        public final AtomicLong totalReads;
        public final AtomicLong totalWrites;
        public final AtomicLong totalReloads;
        public final AtomicLong totalValidations;
        public final AtomicLong validationFailures;
        public final Map<String, AtomicLong> keyReadCounts;
        public final Map<String, AtomicLong> keyWriteCounts;
        public final Map<String, AtomicLong> sourceCounts;
        
        public ConfigStats() {
            this.totalReads = new AtomicLong(0);
            this.totalWrites = new AtomicLong(0);
            this.totalReloads = new AtomicLong(0);
            this.totalValidations = new AtomicLong(0);
            this.validationFailures = new AtomicLong(0);
            this.keyReadCounts = new ConcurrentHashMap<>();
            this.keyWriteCounts = new ConcurrentHashMap<>();
            this.sourceCounts = new ConcurrentHashMap<>();
        }
        
        public void recordRead(String key, ConfigSourceType source) {
            totalReads.incrementAndGet();
            keyReadCounts.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
            sourceCounts.computeIfAbsent(source.name(), k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordWrite(String key, ConfigSourceType source) {
            totalWrites.incrementAndGet();
            keyWriteCounts.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
            sourceCounts.computeIfAbsent(source.name(), k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordReload() {
            totalReloads.incrementAndGet();
        }
        
        public void recordValidation(boolean success) {
            totalValidations.incrementAndGet();
            if (!success) {
                validationFailures.incrementAndGet();
            }
        }
    }
    
    // Configuration manager configuration
    private final ConfigManagerConfig config;
    
    // Configuration entries
    private final Map<String, ConfigEntry> configEntries;
    
    // Configuration validators
    private final Map<String, ConfigValidator> validators;
    
    // Configuration change listeners
    private final List<ConfigChangeListener> listeners;
    
    // Cache for configuration values
    private final Map<String, CacheEntry> configCache;
    
    // Backup configurations
    private final List<Map<String, ConfigEntry>> backups;
    
    // Statistics
    private final ConfigStats stats;
    
    // Lock for configuration management
    private final ReentrantReadWriteLock configLock;
    
    // Scheduled executor for hot reload
    private final ScheduledExecutorService reloadExecutor;
    
    // Shutdown flag
    private volatile boolean shutdown;
    
    // Cache entry
    private static class CacheEntry {
        public final String value;
        public final long timestamp;
        
        public CacheEntry(String value) {
            this.value = value;
            this.timestamp = System.currentTimeMillis();
        }
        
        public boolean isExpired(long timeout) {
            return System.currentTimeMillis() - timestamp > timeout;
        }
    }
    
    /**
     * Constructor
     */
    public ConfigurationManager(ConfigManagerConfig config) {
        this.config = config;
        this.configEntries = new ConcurrentHashMap<>();
        this.validators = new ConcurrentHashMap<>();
        this.listeners = new CopyOnWriteArrayList<>();
        this.configCache = new ConcurrentHashMap<>();
        this.backups = new ArrayList<>();
        this.stats = new ConfigStats();
        this.configLock = new ReentrantReadWriteLock();
        this.reloadExecutor = Executors.newSingleThreadScheduledExecutor();
        this.shutdown = false;
        
        // Start hot reload thread
        if (config.enableHotReload) {
            startHotReloadThread();
        }
    }
    
    /**
     * Default constructor
     */
    public ConfigurationManager() {
        this(new ConfigManagerConfig());
    }
    
    /**
     * Start hot reload thread
     */
    private void startHotReloadThread() {
        reloadExecutor.scheduleAtFixedRate(() -> {
            reloadConfiguration();
        }, config.reloadInterval, config.reloadInterval, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Set configuration value
     */
    public void setConfig(String key, String value) {
        setConfig(key, value, ConfigSourceType.PROGRAMMATIC, null);
    }
    
    /**
     * Set configuration value with source
     */
    public void setConfig(String key, String value, ConfigSourceType source, 
                         String sourceLocation) {
        configLock.writeLock().lock();
        try {
            // Validate if enabled
            if (config.enableValidation) {
                ConfigValidator validator = validators.get(key);
                if (validator != null) {
                    boolean valid = validator.validate(key, value);
                    stats.recordValidation(valid);
                    if (!valid) {
                        throw new IllegalArgumentException("Validation failed: " + 
                            validator.getErrorMessage());
                    }
                }
            }
            
            String oldValue = configEntries.containsKey(key) ? 
                configEntries.get(key).value : null;
            
            ConfigEntry entry = new ConfigEntry(key, value, source, sourceLocation, null);
            configEntries.put(key, entry);
            
            // Invalidate cache
            configCache.remove(key);
            
            // Create backup if enabled
            if (config.enableBackup) {
                createBackup();
            }
            
            stats.recordWrite(key, source);
            
            // Notify listeners
            for (ConfigChangeListener listener : listeners) {
                if (oldValue != null) {
                    listener.onConfigChanged(key, oldValue, value);
                } else {
                    listener.onConfigAdded(key, value);
                }
            }
            
        } finally {
            configLock.writeLock().unlock();
        }
    }
    
    /**
     * Get configuration value
     */
    public String getConfig(String key) {
        configLock.readLock().lock();
        try {
            // Check cache first
            if (config.enableCache) {
                CacheEntry cached = configCache.get(key);
                if (cached != null && !cached.isExpired(config.cacheTimeout)) {
                    return cached.value;
                }
            }
            
            ConfigEntry entry = configEntries.get(key);
            if (entry == null) {
                return null;
            }
            
            // Update cache
            if (config.enableCache) {
                configCache.put(key, new CacheEntry(entry.value));
            }
            
            stats.recordRead(key, entry.source);
            
            return entry.value;
            
        } finally {
            configLock.readLock().unlock();
        }
    }
    
    /**
     * Get configuration value with default
     */
    public String getConfig(String key, String defaultValue) {
        String value = getConfig(key);
        return value != null ? value : defaultValue;
    }
    
    /**
     * Get configuration value as type
     */
    public <T> T getConfigAs(String key, Class<T> type) {
        String value = getConfig(key);
        if (value == null) {
            return null;
        }
        
        ConfigEntry entry = configEntries.get(key);
        if (entry == null) {
            return null;
        }
        
        return entry.getValueAs(type);
    }
    
    /**
     * Get configuration value as type with default
     */
    public <T> T getConfigAs(String key, Class<T> type, T defaultValue) {
        T value = getConfigAs(key, type);
        return value != null ? value : defaultValue;
    }
    
    /**
     * Remove configuration
     */
    public void removeConfig(String key) {
        configLock.writeLock().lock();
        try {
            ConfigEntry removed = configEntries.remove(key);
            if (removed != null) {
                configCache.remove(key);
                
                // Notify listeners
                for (ConfigChangeListener listener : listeners) {
                    listener.onConfigRemoved(key);
                }
            }
        } finally {
            configLock.writeLock().unlock();
        }
    }
    
    /**
     * Check if configuration exists
     */
    public boolean hasConfig(String key) {
        configLock.readLock().lock();
        try {
            return configEntries.containsKey(key);
        } finally {
            configLock.readLock().unlock();
        }
    }
    
    /**
     * Get all configuration keys
     */
    public Set<String> getConfigKeys() {
        configLock.readLock().lock();
        try {
            return new HashSet<>(configEntries.keySet());
        } finally {
            configLock.readLock().unlock();
        }
    }
    
    /**
     * Get all configuration entries
     */
    public Map<String, ConfigEntry> getConfigEntries() {
        configLock.readLock().lock();
        try {
            return new HashMap<>(configEntries);
        } finally {
            configLock.readLock().unlock();
        }
    }
    
    /**
     * Add validator
     */
    public void addValidator(String key, ConfigValidator validator) {
        validators.put(key, validator);
    }
    
    /**
     * Remove validator
     */
    public void removeValidator(String key) {
        validators.remove(key);
    }
    
    /**
     * Add change listener
     */
    public void addChangeListener(ConfigChangeListener listener) {
        listeners.add(listener);
    }
    
    /**
     * Remove change listener
     */
    public void removeChangeListener(ConfigChangeListener listener) {
        listeners.remove(listener);
    }
    
    /**
     * Load configuration from file
     */
    public void loadFromFile(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        List<String> lines = Files.readAllLines(path);
        
        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            
            String[] parts = line.split("=", 2);
            if (parts.length == 2) {
                String key = parts[0].trim();
                String value = parts[1].trim();
                setConfig(key, value, ConfigSourceType.FILE, filePath);
            }
        }
    }
    
    /**
     * Load configuration from environment
     */
    public void loadFromEnvironment() {
        Map<String, String> env = System.getenv();
        for (Map.Entry<String, String> entry : env.entrySet()) {
            if (entry.getKey().startsWith("NEXUS_")) {
                String key = entry.getKey().substring(7).toLowerCase();
                setConfig(key, entry.getValue(), ConfigSourceType.ENVIRONMENT, "environment");
            }
        }
    }
    
    /**
     * Load configuration from system properties
     */
    public void loadFromSystemProperties() {
        Properties props = System.getProperties();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("nexus.")) {
                String configKey = key.substring(7);
                setConfig(configKey, props.getProperty(key), ConfigSourceType.SYSTEM_PROPERTIES, 
                        "system");
            }
        }
    }
    
    /**
     * Save configuration to file
     */
    public void saveToFile(String filePath) throws IOException {
        configLock.readLock().lock();
        try {
            List<String> lines = new ArrayList<>();
            
            for (Map.Entry<String, ConfigEntry> entry : configEntries.entrySet()) {
                lines.add(entry.getKey() + "=" + entry.getValue().value);
            }
            
            Files.write(Paths.get(filePath), lines);
        } finally {
            configLock.readLock().unlock();
        }
    }
    
    /**
     * Create backup
     */
    private void createBackup() {
        Map<String, ConfigEntry> backup = new HashMap<>(configEntries);
        backups.add(backup);
        
        // Keep only specified number of backups
        while (backups.size() > config.backupCount) {
            backups.remove(0);
        }
    }
    
    /**
     * Restore from backup
     */
    public void restoreFromBackup(int backupIndex) {
        if (backupIndex < 0 || backupIndex >= backups.size()) {
            throw new IllegalArgumentException("Invalid backup index: " + backupIndex);
        }
        
        configLock.writeLock().lock();
        try {
            Map<String, ConfigEntry> backup = backups.get(backupIndex);
            configEntries.clear();
            configEntries.putAll(backup);
            configCache.clear();
        } finally {
            configLock.writeLock().unlock();
        }
    }
    
    /**
     * Reload configuration
     */
    public void reloadConfiguration() {
        stats.recordReload();
        
        // In a real implementation, this would reload from all configured sources
        // For now, we just clear the cache
        configCache.clear();
    }
    
    /**
     * Clear cache
     */
    public void clearCache() {
        configCache.clear();
    }
    
    /**
     * Get statistics
     */
    public ConfigStats getStats() {
        return stats;
    }
    
    /**
     * Get configuration
     */
    public ConfigManagerConfig getConfig() {
        return config;
    }
    
    /**
     * Get backup count
     */
    public int getBackupCount() {
        return backups.size();
    }
    
    /**
     * Shutdown configuration manager
     */
    public void shutdown() {
        shutdown = true;
        
        reloadExecutor.shutdown();
        try {
            reloadExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        configEntries.clear();
        configCache.clear();
        backups.clear();
        validators.clear();
        listeners.clear();
    }
}
