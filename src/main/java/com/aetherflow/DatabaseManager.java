package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.sql.*;
import javax.sql.DataSource;
import java.io.*;

/**
 * Database manager for AetherFlow.
 * Handles database connections, query execution, transaction management, and connection pooling.
 * Supports multiple database types and provides comprehensive query capabilities.
 */
public class DatabaseManager {
    
    // Database type
    public enum DatabaseType {
        MYSQL,
        POSTGRESQL,
        SQLITE,
        ORACLE,
        SQLSERVER,
        H2,
        CUSTOM
    }
    
    // Query result
    public static class QueryResult {
        public final List<Map<String, Object>> rows;
        public final int affectedRows;
        public final long executionTime;
        public final String query;
        public final Map<String, Object> parameters;
        public final boolean success;
        public final String errorMessage;
        
        public QueryResult(List<Map<String, Object>> rows, int affectedRows, long executionTime,
                         String query, Map<String, Object> parameters, boolean success, 
                         String errorMessage) {
            this.rows = rows != null ? new ArrayList<>(rows) : new ArrayList<>();
            this.affectedRows = affectedRows;
            this.executionTime = executionTime;
            this.query = query;
            this.parameters = parameters != null ? new HashMap<>(parameters) : new HashMap<>();
            this.success = success;
            this.errorMessage = errorMessage;
        }
        
        public static QueryResult success(List<Map<String, Object>> rows, int affectedRows, 
                                        long executionTime, String query, 
                                        Map<String, Object> parameters) {
            return new QueryResult(rows, affectedRows, executionTime, query, parameters, true, null);
        }
        
        public static QueryResult failure(String query, Map<String, Object> parameters, 
                                        String errorMessage) {
            return new QueryResult(null, 0, 0, query, parameters, false, errorMessage);
        }
    }
    
    // Database configuration
    public static class DatabaseConfig {
        public DatabaseType databaseType;
        public String host;
        public int port;
        public String databaseName;
        public String username;
        public String password;
        public int maxConnections;
        public int minConnections;
        public long connectionTimeout;
        public long queryTimeout;
        public boolean enableConnectionPool;
        public boolean enableStatementCache;
        public int statementCacheSize;
        public boolean enableQueryLogging;
        public boolean enableTransactionSupport;
        public String connectionString;
        public Map<String, String> connectionProperties;
        
        public DatabaseConfig() {
            this.databaseType = DatabaseType.H2;
            this.host = "localhost";
            this.port = 3306;
            this.databaseName = "aetherflow";
            this.username = "sa";
            this.password = "";
            this.maxConnections = 10;
            this.minConnections = 2;
            this.connectionTimeout = 30000;
            this.queryTimeout = 60000;
            this.enableConnectionPool = true;
            this.enableStatementCache = true;
            this.statementCacheSize = 100;
            this.enableQueryLogging = true;
            this.enableTransactionSupport = true;
            this.connectionString = null;
            this.connectionProperties = new HashMap<>();
        }
    }
    
    // Database statistics
    public static class DatabaseStats {
        public final AtomicLong totalQueries;
        public final AtomicLong successfulQueries;
        public final AtomicLong failedQueries;
        public final AtomicLong totalTransactions;
        public final AtomicLong committedTransactions;
        public final AtomicLong rolledBackTransactions;
        public final AtomicLong totalConnectionsCreated;
        public final AtomicLong totalConnectionsClosed;
        public final AtomicLong activeConnections;
        public final AtomicLong totalQueryTime;
        public final AtomicLong averageQueryTime;
        public final Map<String, AtomicLong> queryTypeCounts;
        public final Map<String, AtomicLong> errorCounts;
        
        public DatabaseStats() {
            this.totalQueries = new AtomicLong(0);
            this.successfulQueries = new AtomicLong(0);
            this.failedQueries = new AtomicLong(0);
            this.totalTransactions = new AtomicLong(0);
            this.committedTransactions = new AtomicLong(0);
            this.rolledBackTransactions = new AtomicLong(0);
            this.totalConnectionsCreated = new AtomicLong(0);
            this.totalConnectionsClosed = new AtomicLong(0);
            this.activeConnections = new AtomicLong(0);
            this.totalQueryTime = new AtomicLong(0);
            this.averageQueryTime = new AtomicLong(0);
            this.queryTypeCounts = new ConcurrentHashMap<>();
            this.errorCounts = new ConcurrentHashMap<>();
        }
        
        public void recordQuery(String queryType, long executionTime, boolean success) {
            totalQueries.incrementAndGet();
            if (success) {
                successfulQueries.incrementAndGet();
            } else {
                failedQueries.incrementAndGet();
            }
            totalQueryTime.addAndGet(executionTime);
            
            long total = totalQueries.get();
            long newAvg = totalQueryTime.get() / total;
            averageQueryTime.set(newAvg);
            
            queryTypeCounts.computeIfAbsent(queryType, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordTransaction(boolean committed) {
            totalTransactions.incrementAndGet();
            if (committed) {
                committedTransactions.incrementAndGet();
            } else {
                rolledBackTransactions.incrementAndGet();
            }
        }
        
        public void recordConnectionCreated() {
            totalConnectionsCreated.incrementAndGet();
            activeConnections.incrementAndGet();
        }
        
        public void recordConnectionClosed() {
            totalConnectionsClosed.incrementAndGet();
            activeConnections.decrementAndGet();
        }
        
        public void recordError(String errorType) {
            errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
        }
    }
    
    // Connection wrapper
    private static class ConnectionWrapper {
        public final Connection connection;
        public final long creationTime;
        public volatile long lastUsed;
        public volatile boolean inUse;
        public final ReentrantLock connectionLock;
        
        public ConnectionWrapper(Connection connection) {
            this.connection = connection;
            this.creationTime = System.currentTimeMillis();
            this.lastUsed = creationTime;
            this.inUse = false;
            this.connectionLock = new ReentrantLock();
        }
        
        public long getAge() {
            return System.currentTimeMillis() - creationTime;
        }
        
        public long getIdleTime() {
            return System.currentTimeMillis() - lastUsed;
        }
        
        public void recordUsage() {
            lastUsed = System.currentTimeMillis();
        }
    }
    
    // Transaction context
    private static class TransactionContext {
        public final String transactionId;
        public final Connection connection;
        public volatile boolean active;
        public final long creationTime;
        public final List<String> executedQueries;
        
        public TransactionContext(String transactionId, Connection connection) {
            this.transactionId = transactionId;
            this.connection = connection;
            this.active = true;
            this.creationTime = System.currentTimeMillis();
            this.executedQueries = new ArrayList<>();
        }
        
        public void addQuery(String query) {
            executedQueries.add(query);
        }
    }
    
    // Database configuration
    private final DatabaseConfig config;
    
    // Connection pool
    private final BlockingQueue<ConnectionWrapper> connectionPool;
    
    // Active connections
    private final Set<ConnectionWrapper> activeConnections;
    
    // Transaction contexts
    private final Map<String, TransactionContext> transactions;
    
    // Statement cache
    private final Map<String, PreparedStatement> statementCache;
    
    // Statistics
    private final DatabaseStats stats;
    
    // Lock for connection management
    private final ReentrantLock connectionLock;
    
    // Lock for transaction management
    private final ReentrantLock transactionLock;
    
    // Lock for statement cache
    private final ReentrantLock statementLock;
    
    // Transaction ID generator
    private final AtomicLong transactionIdGenerator;
    
    // Scheduled executor for cleanup
    private final ScheduledExecutorService cleanupExecutor;
    
    // Shutdown flag
    private volatile boolean shutdown;
    
    /**
     * Constructor
     */
    public DatabaseManager(DatabaseConfig config) throws SQLException {
        this.config = config;
        this.connectionPool = new LinkedBlockingQueue<>(config.maxConnections);
        this.activeConnections = new HashSet<>();
        this.transactions = new ConcurrentHashMap<>();
        this.statementCache = new ConcurrentHashMap<>();
        this.stats = new DatabaseStats();
        this.connectionLock = new ReentrantLock();
        this.transactionLock = new ReentrantLock();
        this.statementLock = new ReentrantLock();
        this.transactionIdGenerator = new AtomicLong(0);
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        this.shutdown = false;
        
        // Initialize connection pool
        initializeConnectionPool();
        
        // Start cleanup thread
        startCleanupThread();
    }
    
    /**
     * Default constructor
     */
    public DatabaseManager() throws SQLException {
        this(new DatabaseConfig());
    }
    
    /**
     * Initialize connection pool
     */
    private void initializeConnectionPool() throws SQLException {
        for (int i = 0; i < config.minConnections; i++) {
            ConnectionWrapper wrapper = createConnection();
            if (wrapper != null) {
                connectionPool.offer(wrapper);
            }
        }
    }
    
    /**
     * Create database connection
     */
    private ConnectionWrapper createConnection() throws SQLException {
        try {
            Connection connection;
            
            if (config.connectionString != null) {
                Properties props = new Properties();
                props.putAll(config.connectionProperties);
                connection = DriverManager.getConnection(config.connectionString, props);
            } else {
                String url = buildConnectionString();
                connection = DriverManager.getConnection(url, config.username, config.password);
            }
            
            connection.setAutoCommit(!config.enableTransactionSupport);
            
            ConnectionWrapper wrapper = new ConnectionWrapper(connection);
            stats.recordConnectionCreated();
            
            return wrapper;
            
        } catch (SQLException e) {
            stats.recordError("Connection creation failed");
            throw e;
        }
    }
    
    /**
     * Build connection string
     */
    private String buildConnectionString() {
        switch (config.databaseType) {
            case MYSQL:
                return String.format("jdbc:mysql://%s:%d/%s", config.host, config.port, 
                                   config.databaseName);
            case POSTGRESQL:
                return String.format("jdbc:postgresql://%s:%d/%s", config.host, config.port, 
                                   config.databaseName);
            case SQLITE:
                return String.format("jdbc:sqlite:%s", config.databaseName);
            case ORACLE:
                return String.format("jdbc:oracle:thin:@%s:%d:%s", config.host, config.port, 
                                   config.databaseName);
            case SQLSERVER:
                return String.format("jdbc:sqlserver://%s:%d;databaseName=%s", config.host, 
                                   config.port, config.databaseName);
            case H2:
                return String.format("jdbc:h2:mem:%s", config.databaseName);
            default:
                throw new IllegalArgumentException("Unsupported database type: " + config.databaseType);
        }
    }
    
    /**
     * Start cleanup thread
     */
    private void startCleanupThread() {
        cleanupExecutor.scheduleAtFixedRate(() -> {
            cleanupIdleConnections();
            cleanupExpiredTransactions();
        }, 60000, 60000, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Get connection from pool
     */
    private ConnectionWrapper getConnection() throws SQLException {
        if (shutdown) {
            throw new SQLException("Database manager is shutdown");
        }
        
        connectionLock.lock();
        try {
            ConnectionWrapper wrapper = connectionPool.poll();
            
            if (wrapper == null) {
                if (activeConnections.size() < config.maxConnections) {
                    wrapper = createConnection();
                } else {
                    // Wait for available connection
                    wrapper = connectionPool.poll(config.connectionTimeout, TimeUnit.MILLISECONDS);
                    if (wrapper == null) {
                        throw new SQLException("Connection timeout");
                    }
                }
            }
            
            wrapper.inUse = true;
            wrapper.recordUsage();
            activeConnections.add(wrapper);
            
            return wrapper;
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while waiting for connection", e);
        } finally {
            connectionLock.unlock();
        }
    }
    
    /**
     * Return connection to pool
     */
    private void returnConnection(ConnectionWrapper wrapper) {
        connectionLock.lock();
        try {
            wrapper.inUse = false;
            activeConnections.remove(wrapper);
            
            if (wrapper.connection.isValid((int) config.connectionTimeout)) {
                connectionPool.offer(wrapper);
            } else {
                try {
                    wrapper.connection.close();
                    stats.recordConnectionClosed();
                } catch (SQLException e) {
                    // Ignore close errors
                }
            }
        } catch (SQLException e) {
            // Connection is invalid, close it
            try {
                wrapper.connection.close();
                stats.recordConnectionClosed();
            } catch (SQLException ex) {
                // Ignore close errors
            }
        } finally {
            connectionLock.unlock();
        }
    }
    
    /**
     * Execute query
     */
    public QueryResult executeQuery(String query) throws SQLException {
        return executeQuery(query, null);
    }
    
    /**
     * Execute query with parameters
     */
    public QueryResult executeQuery(String query, Map<String, Object> parameters) throws SQLException {
        ConnectionWrapper wrapper = null;
        long startTime = System.currentTimeMillis();
        
        try {
            wrapper = getConnection();
            
            PreparedStatement statement = prepareStatement(wrapper.connection, query, parameters);
            
            ResultSet resultSet = statement.executeQuery();
            
            List<Map<String, Object>> rows = new ArrayList<>();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = resultSet.getObject(i);
                    row.put(columnName, value);
                }
                rows.add(row);
            }
            
            resultSet.close();
            statement.close();
            
            long executionTime = System.currentTimeMillis() - startTime;
            stats.recordQuery("SELECT", executionTime, true);
            
            return QueryResult.success(rows, 0, executionTime, query, parameters);
            
        } catch (SQLException e) {
            long executionTime = System.currentTimeMillis() - startTime;
            stats.recordQuery("SELECT", executionTime, false);
            stats.recordError(e.getClass().getSimpleName());
            
            return QueryResult.failure(query, parameters, e.getMessage());
            
        } finally {
            if (wrapper != null) {
                returnConnection(wrapper);
            }
        }
    }
    
    /**
     * Execute update
     */
    public QueryResult executeUpdate(String query) throws SQLException {
        return executeUpdate(query, null);
    }
    
    /**
     * Execute update with parameters
     */
    public QueryResult executeUpdate(String query, Map<String, Object> parameters) throws SQLException {
        ConnectionWrapper wrapper = null;
        long startTime = System.currentTimeMillis();
        
        try {
            wrapper = getConnection();
            
            PreparedStatement statement = prepareStatement(wrapper.connection, query, parameters);
            
            int affectedRows = statement.executeUpdate();
            
            statement.close();
            
            long executionTime = System.currentTimeMillis() - startTime;
            stats.recordQuery("UPDATE", executionTime, true);
            
            return QueryResult.success(null, affectedRows, executionTime, query, parameters);
            
        } catch (SQLException e) {
            long executionTime = System.currentTimeMillis() - startTime;
            stats.recordQuery("UPDATE", executionTime, false);
            stats.recordError(e.getClass().getSimpleName());
            
            return QueryResult.failure(query, parameters, e.getMessage());
            
        } finally {
            if (wrapper != null) {
                returnConnection(wrapper);
            }
        }
    }
    
    /**
     * Prepare statement
     */
    private PreparedStatement prepareStatement(Connection connection, String query, 
                                             Map<String, Object> parameters) throws SQLException {
        PreparedStatement statement;
        
        if (config.enableStatementCache && parameters == null) {
            statement = statementCache.computeIfAbsent(query, k -> {
                try {
                    return connection.prepareStatement(k);
                } catch (SQLException e) {
                    return null;
                }
            });
            
            if (statement == null || statement.isClosed()) {
                statement = connection.prepareStatement(query);
                statementCache.put(query, statement);
            }
        } else {
            statement = connection.prepareStatement(query);
        }
        
        // Set parameters
        if (parameters != null) {
            int index = 1;
            for (Object value : parameters.values()) {
                statement.setObject(index++, value);
            }
        }
        
        return statement;
    }
    
    /**
     * Begin transaction
     */
    public String beginTransaction() throws SQLException {
        ConnectionWrapper wrapper = null;
        
        try {
            wrapper = getConnection();
            wrapper.connection.setAutoCommit(false);
            
            String transactionId = "tx_" + transactionIdGenerator.incrementAndGet() + "_" + 
                                 System.currentTimeMillis();
            
            TransactionContext context = new TransactionContext(transactionId, wrapper.connection);
            
            transactionLock.lock();
            try {
                transactions.put(transactionId, context);
            } finally {
                transactionLock.unlock();
            }
            
            return transactionId;
            
        } catch (SQLException e) {
            if (wrapper != null) {
                returnConnection(wrapper);
            }
            stats.recordError("Transaction begin failed");
            throw e;
        }
    }
    
    /**
     * Commit transaction
     */
    public void commitTransaction(String transactionId) throws SQLException {
        transactionLock.lock();
        try {
            TransactionContext context = transactions.remove(transactionId);
            if (context == null) {
                throw new SQLException("Transaction not found: " + transactionId);
            }
            
            context.connection.commit();
            context.active = false;
            
            stats.recordTransaction(true);
            
            // Return connection to pool
            ConnectionWrapper wrapper = findWrapperForConnection(context.connection);
            if (wrapper != null) {
                returnConnection(wrapper);
            }
            
        } catch (SQLException e) {
            stats.recordError("Transaction commit failed");
            throw e;
        } finally {
            transactionLock.unlock();
        }
    }
    
    /**
     * Rollback transaction
     */
    public void rollbackTransaction(String transactionId) throws SQLException {
        transactionLock.lock();
        try {
            TransactionContext context = transactions.remove(transactionId);
            if (context == null) {
                throw new SQLException("Transaction not found: " + transactionId);
            }
            
            context.connection.rollback();
            context.active = false;
            
            stats.recordTransaction(false);
            
            // Return connection to pool
            ConnectionWrapper wrapper = findWrapperForConnection(context.connection);
            if (wrapper != null) {
                returnConnection(wrapper);
            }
            
        } catch (SQLException e) {
            stats.recordError("Transaction rollback failed");
            throw e;
        } finally {
            transactionLock.unlock();
        }
    }
    
    /**
     * Find wrapper for connection
     */
    private ConnectionWrapper findWrapperForConnection(Connection connection) {
        for (ConnectionWrapper wrapper : activeConnections) {
            if (wrapper.connection == connection) {
                return wrapper;
            }
        }
        return null;
    }
    
    /**
     * Execute query in transaction
     */
    public QueryResult executeInTransaction(String transactionId, String query, 
                                           Map<String, Object> parameters) throws SQLException {
        transactionLock.lock();
        long startTime = System.currentTimeMillis();
        try {
            TransactionContext context = transactions.get(transactionId);
            if (context == null) {
                throw new SQLException("Transaction not found: " + transactionId);
            }
            
            if (!context.active) {
                throw new SQLException("Transaction is not active: " + transactionId);
            }
            
            long txStartTime = System.currentTimeMillis();
            
            PreparedStatement statement = prepareStatement(context.connection, query, parameters);
            
            context.addQuery(query);
            
            boolean isQuery = query.trim().toUpperCase().startsWith("SELECT");
            
            if (isQuery) {
                ResultSet resultSet = statement.executeQuery();
                
                List<Map<String, Object>> rows = new ArrayList<>();
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                while (resultSet.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object value = resultSet.getObject(i);
                        row.put(columnName, value);
                    }
                    rows.add(row);
                }
                
                resultSet.close();
                statement.close();
                
                long executionTime = System.currentTimeMillis() - startTime;
                stats.recordQuery("SELECT", executionTime, true);
                
                return QueryResult.success(rows, 0, executionTime, query, parameters);
                
            } else {
                int affectedRows = statement.executeUpdate();
                statement.close();
                
                long executionTime = System.currentTimeMillis() - startTime;
                stats.recordQuery("UPDATE", executionTime, true);
                
                return QueryResult.success(null, affectedRows, executionTime, query, parameters);
            }
            
        } catch (SQLException e) {
            long executionTime = System.currentTimeMillis() - startTime;
            stats.recordQuery("UPDATE", executionTime, false);
            stats.recordError(e.getClass().getSimpleName());
            
            return QueryResult.failure(query, parameters, e.getMessage());
            
        } finally {
            transactionLock.unlock();
        }
    }
    
    /**
     * Cleanup idle connections
     */
    private void cleanupIdleConnections() {
        connectionLock.lock();
        try {
            Iterator<ConnectionWrapper> it = connectionPool.iterator();
            while (it.hasNext()) {
                ConnectionWrapper wrapper = it.next();
                if (wrapper.getIdleTime() > config.connectionTimeout * 2) {
                    it.remove();
                    try {
                        wrapper.connection.close();
                        stats.recordConnectionClosed();
                    } catch (SQLException e) {
                        // Ignore close errors
                    }
                }
            }
        } finally {
            connectionLock.unlock();
        }
    }
    
    /**
     * Cleanup expired transactions
     */
    private void cleanupExpiredTransactions() {
        long now = System.currentTimeMillis();
        
        transactionLock.lock();
        try {
            Iterator<Map.Entry<String, TransactionContext>> it = transactions.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, TransactionContext> entry = it.next();
                TransactionContext context = entry.getValue();
                
                if (now - context.creationTime > config.queryTimeout) {
                    try {
                        context.connection.rollback();
                    } catch (SQLException e) {
                        // Ignore rollback errors
                    }
                    context.active = false;
                    it.remove();
                    stats.recordTransaction(false);
                }
            }
        } finally {
            transactionLock.unlock();
        }
    }
    
    /**
     * Get statistics
     */
    public DatabaseStats getStats() {
        return stats;
    }
    
    /**
     * Get configuration
     */
    public DatabaseConfig getConfig() {
        return config;
    }
    
    /**
     * Get active connection count
     */
    public int getActiveConnectionCount() {
        return activeConnections.size();
    }
    
    /**
     * Get available connection count
     */
    public int getAvailableConnectionCount() {
        return connectionPool.size();
    }
    
    /**
     * Get transaction count
     */
    public int getTransactionCount() {
        return transactions.size();
    }
    
    /**
     * Clear statement cache
     */
    public void clearStatementCache() {
        statementLock.lock();
        try {
            for (PreparedStatement statement : statementCache.values()) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    // Ignore close errors
                }
            }
            statementCache.clear();
        } finally {
            statementLock.unlock();
        }
    }
    
    /**
     * Shutdown database manager
     */
    public void shutdown() {
        shutdown = true;
        
        // Rollback all active transactions
        transactionLock.lock();
        try {
            for (TransactionContext context : transactions.values()) {
                try {
                    context.connection.rollback();
                } catch (SQLException e) {
                    // Ignore rollback errors
                }
            }
            transactions.clear();
        } finally {
            transactionLock.unlock();
        }
        
        // Close all connections
        connectionLock.lock();
        try {
            for (ConnectionWrapper wrapper : connectionPool) {
                try {
                    wrapper.connection.close();
                    stats.recordConnectionClosed();
                } catch (SQLException e) {
                    // Ignore close errors
                }
            }
            connectionPool.clear();
            
            for (ConnectionWrapper wrapper : activeConnections) {
                try {
                    wrapper.connection.close();
                    stats.recordConnectionClosed();
                } catch (SQLException e) {
                    // Ignore close errors
                }
            }
            activeConnections.clear();
        } finally {
            connectionLock.unlock();
        }
        
        // Clear statement cache
        clearStatementCache();
        
        // Shutdown cleanup executor
        cleanupExecutor.shutdown();
        try {
            cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Database exception
     */
    public static class DatabaseException extends RuntimeException {
        public DatabaseException(String message) {
            super(message);
        }
        
        public DatabaseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
