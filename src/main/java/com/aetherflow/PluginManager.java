package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.io.*;
import java.net.*;
import java.nio.file.*;

/**
 * Plugin manager for AetherFlow.
 * Manages plugin lifecycle, loading, unloading, and communication.
 * Supports dynamic plugin discovery and hot reloading.
 */
public class PluginManager {
    
    // Plugin state
    public enum PluginState {
        LOADED,
        INITIALIZED,
        STARTED,
        STOPPED,
        UNLOADED,
        ERROR
    }
    
    // Plugin descriptor
    public static class PluginDescriptor {
        public final String pluginId;
        public final String pluginName;
        public final String version;
        public final String description;
        public final String mainClass;
        public final Set<String> dependencies;
        public final Map<String, String> properties;
        public final Path pluginPath;
        
        public PluginDescriptor(String pluginId, String pluginName, String version, 
                             String description, String mainClass, Set<String> dependencies,
                             Map<String, String> properties, Path pluginPath) {
            this.pluginId = pluginId;
            this.pluginName = pluginName;
            this.version = version;
            this.description = description;
            this.mainClass = mainClass;
            this.dependencies = dependencies != null ? new HashSet<>(dependencies) : new HashSet<>();
            this.properties = properties != null ? new HashMap<>(properties) : new HashMap<>();
            this.pluginPath = pluginPath;
        }
    }
    
    // Plugin instance
    public static class PluginInstance {
        public final PluginDescriptor descriptor;
        public volatile PluginState state;
        public volatile Object pluginObject;
        public volatile long loadTime;
        public volatile long startTime;
        public volatile long stopTime;
        public final Map<String, Object> pluginData;
        public final ReentrantLock pluginLock;
        
        public PluginInstance(PluginDescriptor descriptor) {
            this.descriptor = descriptor;
            this.state = PluginState.LOADED;
            this.pluginObject = null;
            this.loadTime = System.currentTimeMillis();
            this.pluginData = new ConcurrentHashMap<>();
            this.pluginLock = new ReentrantLock();
        }
        
        public long getUptime() {
            if (state == PluginState.STARTED) {
                return System.currentTimeMillis() - startTime;
            } else if (state == PluginState.STOPPED) {
                return stopTime - startTime;
            }
            return 0;
        }
    }
    
    // Plugin manager configuration
    public static class PluginManagerConfig {
        public Path pluginDirectory;
        public boolean enableHotReload;
        public long reloadInterval;
        public boolean enableDependencyResolution;
        public int maxLoadAttempts;
        public long loadTimeout;
        public boolean enablePluginIsolation;
        public boolean enableMetrics;
        
        public PluginManagerConfig() {
            this.pluginDirectory = Paths.get("plugins");
            this.enableHotReload = true;
            this.reloadInterval = 60000; // 1 minute
            this.enableDependencyResolution = true;
            this.maxLoadAttempts = 3;
            this.loadTimeout = 30000; // 30 seconds
            this.enablePluginIsolation = false;
            this.enableMetrics = true;
        }
    }
    
    // Plugin manager statistics
    public static class PluginManagerStats {
        public final AtomicLong totalPluginsLoaded;
        public final AtomicLong totalPluginsUnloaded;
        public final AtomicLong totalPluginErrors;
        public final AtomicLong totalPluginStarts;
        public final AtomicLong totalPluginStops;
        public final Map<String, AtomicLong> pluginTypeCounts;
        public final Map<String, AtomicLong> errorCounts;
        
        public PluginManagerStats() {
            this.totalPluginsLoaded = new AtomicLong(0);
            this.totalPluginsUnloaded = new AtomicLong(0);
            this.totalPluginErrors = new AtomicLong(0);
            this.totalPluginStarts = new AtomicLong(0);
            this.totalPluginStops = new AtomicLong(0);
            this.pluginTypeCounts = new ConcurrentHashMap<>();
            this.errorCounts = new ConcurrentHashMap<>();
        }
        
        public void recordPluginLoad(String pluginType) {
            totalPluginsLoaded.incrementAndGet();
            pluginTypeCounts.computeIfAbsent(pluginType, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordPluginUnload() {
            totalPluginsUnloaded.incrementAndGet();
        }
        
        public void recordPluginError(String errorType) {
            totalPluginErrors.incrementAndGet();
            errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordPluginStart() {
            totalPluginStarts.incrementAndGet();
        }
        
        public void recordPluginStop() {
            totalPluginStops.incrementAndGet();
        }
    }
    
    // Plugin manager configuration
    private final PluginManagerConfig config;
    
    // Plugin instances
    private final Map<String, PluginInstance> plugins;
    
    // Plugin descriptors
    private final Map<String, PluginDescriptor> pluginDescriptors;
    
    // Class loader for plugins
    private volatile ClassLoader pluginClassLoader;
    
    // Statistics
    private final PluginManagerStats stats;
    
    // Lock for plugin management
    private final ReentrantLock pluginLock;
    
    // Scheduled executor for hot reload
    private final ScheduledExecutorService reloadExecutor;
    
    // Shutdown flag
    private volatile boolean shutdown;
    
    /**
     * Constructor
     */
    public PluginManager(PluginManagerConfig config) {
        this.config = config;
        this.plugins = new ConcurrentHashMap<>();
        this.pluginDescriptors = new ConcurrentHashMap<>();
        this.stats = new PluginManagerStats();
        this.pluginLock = new ReentrantLock();
        this.reloadExecutor = Executors.newSingleThreadScheduledExecutor();
        this.shutdown = false;
        
        // Create plugin directory if it doesn't exist
        try {
            if (!Files.exists(config.pluginDirectory)) {
                Files.createDirectories(config.pluginDirectory);
            }
        } catch (IOException e) {
            // Ignore directory creation errors
        }
        
        // Start hot reload thread
        if (config.enableHotReload) {
            startHotReloadThread();
        }
    }
    
    /**
     * Default constructor
     */
    public PluginManager() {
        this(new PluginManagerConfig());
    }
    
    /**
     * Start hot reload thread
     */
    private void startHotReloadThread() {
        reloadExecutor.scheduleAtFixedRate(() -> {
            scanForPlugins();
        }, config.reloadInterval, config.reloadInterval, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Scan for plugins
     */
    public void scanForPlugins() {
        if (!Files.exists(config.pluginDirectory)) {
            return;
        }
        
        try {
            Files.walk(config.pluginDirectory)
                 .filter(path -> path.toString().endsWith(".jar"))
                 .forEach(this::loadPluginFromPath);
        } catch (IOException e) {
            stats.recordPluginError("Scan failed");
        }
    }
    
    /**
     * Load plugin from path
     */
    public void loadPluginFromPath(Path pluginPath) {
        // Can lead to code injection or malicious plugin execution
        // In a real implementation, this would validate the plugin signature and manifest
        String pluginId = pluginPath.getFileName().toString();
        
        if (plugins.containsKey(pluginId)) {
            return;
        }
        
        PluginDescriptor descriptor = new PluginDescriptor(
            pluginId,
            pluginId,
            "1.0.0",
            "Auto-discovered plugin",
            null,
            new HashSet<>(),
            new HashMap<>(),
            pluginPath
        );
        
        loadPlugin(descriptor);
    }
    
    /**
     * Load plugin
     */
    public void loadPlugin(PluginDescriptor descriptor) {
        pluginLock.lock();
        try {
            if (plugins.containsKey(descriptor.pluginId)) {
                return;
            }
            
            PluginInstance instance = new PluginInstance(descriptor);
            plugins.put(descriptor.pluginId, instance);
            pluginDescriptors.put(descriptor.pluginId, descriptor);
            
            stats.recordPluginLoad(descriptor.pluginName);
            
            // Initialize plugin if main class is specified
            if (descriptor.mainClass != null) {
                initializePlugin(instance);
            }
            
        } finally {
            pluginLock.unlock();
        }
    }
    
    /**
     * Initialize plugin
     */
    private void initializePlugin(PluginInstance instance) {
        instance.pluginLock.lock();
        try {
            // Can lead to class loading attacks
            if (pluginClassLoader == null) {
                pluginClassLoader = createPluginClassLoader();
            }
            
            Class<?> pluginClass = pluginClassLoader.loadClass(instance.descriptor.mainClass);
            Object pluginObject = pluginClass.getDeclaredConstructor().newInstance();
            
            instance.pluginObject = pluginObject;
            instance.state = PluginState.INITIALIZED;
            
            // Start plugin
            startPlugin(instance);
            
        } catch (Exception e) {
            instance.state = PluginState.ERROR;
            stats.recordPluginError(e.getClass().getSimpleName());
        } finally {
            instance.pluginLock.unlock();
        }
    }
    
    /**
     * Create plugin class loader
     */
    private ClassLoader createPluginClassLoader() {
        return new URLClassLoader(new URL[0], getClass().getClassLoader());
    }
    
    /**
     * Start plugin
     */
    public void startPlugin(String pluginId) {
        pluginLock.lock();
        try {
            PluginInstance instance = plugins.get(pluginId);
            if (instance == null) {
                return;
            }
            
            startPlugin(instance);
            
        } finally {
            pluginLock.unlock();
        }
    }
    
    /**
     * Start plugin instance
     */
    private void startPlugin(PluginInstance instance) {
        instance.pluginLock.lock();
        try {
            if (instance.state == PluginState.STARTED) {
                return;
            }
            
            // Call start method if it exists
            if (instance.pluginObject != null) {
                try {
                    instance.pluginObject.getClass().getMethod("start").invoke(instance.pluginObject);
                } catch (NoSuchMethodException e) {
                    // Plugin doesn't have start method, that's okay
                }
            }
            
            instance.state = PluginState.STARTED;
            instance.startTime = System.currentTimeMillis();
            stats.recordPluginStart();
            
        } catch (Exception e) {
            instance.state = PluginState.ERROR;
            stats.recordPluginError(e.getClass().getSimpleName());
        } finally {
            instance.pluginLock.unlock();
        }
    }
    
    /**
     * Stop plugin
     */
    public void stopPlugin(String pluginId) {
        pluginLock.lock();
        try {
            PluginInstance instance = plugins.get(pluginId);
            if (instance == null) {
                return;
            }
            
            instance.pluginLock.lock();
            try {
                if (instance.state != PluginState.STARTED) {
                    return;
                }
                
                // Call stop method if it exists
                if (instance.pluginObject != null) {
                    try {
                        instance.pluginObject.getClass().getMethod("stop").invoke(instance.pluginObject);
                    } catch (NoSuchMethodException e) {
                        // Plugin doesn't have stop method, that's okay
                    }
                }
                
                instance.state = PluginState.STOPPED;
                instance.stopTime = System.currentTimeMillis();
                stats.recordPluginStop();
                
            } catch (Exception e) {
                instance.state = PluginState.ERROR;
                stats.recordPluginError(e.getClass().getSimpleName());
            } finally {
                instance.pluginLock.unlock();
            }
            
        } finally {
            pluginLock.unlock();
        }
    }
    
    /**
     * Unload plugin
     */
    public void unloadPlugin(String pluginId) {
        pluginLock.lock();
        try {
            PluginInstance instance = plugins.remove(pluginId);
            if (instance != null) {
                // Stop plugin if it's running
                if (instance.state == PluginState.STARTED) {
                    stopPlugin(pluginId);
                }
                
                instance.state = PluginState.UNLOADED;
                pluginDescriptors.remove(pluginId);
                stats.recordPluginUnload();
            }
        } finally {
            pluginLock.unlock();
        }
    }
    
    /**
     * Get plugin instance
     */
    public PluginInstance getPlugin(String pluginId) {
        return plugins.get(pluginId);
    }
    
    /**
     * Get plugin descriptor
     */
    public PluginDescriptor getPluginDescriptor(String pluginId) {
        return pluginDescriptors.get(pluginId);
    }
    
    /**
     * Get all plugins
     */
    public Collection<PluginInstance> getPlugins() {
        return new ArrayList<>(plugins.values());
    }
    
    /**
     * Get plugins by state
     */
    public List<PluginInstance> getPluginsByState(PluginState state) {
        List<PluginInstance> result = new ArrayList<>();
        for (PluginInstance instance : plugins.values()) {
            if (instance.state == state) {
                result.add(instance);
            }
        }
        return result;
    }
    
    /**
     * Get plugin object
     */
    public Object getPluginObject(String pluginId) {
        PluginInstance instance = plugins.get(pluginId);
        return instance != null ? instance.pluginObject : null;
    }
    
    /**
     * Set plugin data
     */
    public void setPluginData(String pluginId, String key, Object value) {
        PluginInstance instance = plugins.get(pluginId);
        if (instance != null) {
            instance.pluginData.put(key, value);
        }
    }
    
    /**
     * Get plugin data
     */
    public Object getPluginData(String pluginId, String key) {
        PluginInstance instance = plugins.get(pluginId);
        return instance != null ? instance.pluginData.get(key) : null;
    }
    
    /**
     * Get statistics
     */
    public PluginManagerStats getStats() {
        return stats;
    }
    
    /**
     * Get configuration
     */
    public PluginManagerConfig getConfig() {
        return config;
    }
    
    /**
     * Shutdown plugin manager
     */
    public void shutdown() {
        shutdown = true;
        
        // Stop all plugins
        for (String pluginId : new ArrayList<>(plugins.keySet())) {
            stopPlugin(pluginId);
        }
        
        // Unload all plugins
        for (String pluginId : new ArrayList<>(plugins.keySet())) {
            unloadPlugin(pluginId);
        }
        
        reloadExecutor.shutdown();
        try {
            reloadExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        plugins.clear();
        pluginDescriptors.clear();
    }
}
