package com.aetherflow;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Network transport layer for AetherFlow.
 * Handles low-level network I/O, connection management, and data transmission.
 * Supports TCP, UDP, and Unix domain sockets with multiplexing.
 */
public class NetworkTransport {
    
    // Transport types
    public enum TransportType {
        TCP,
        UDP,
        UNIX_DOMAIN
    }
    
    // Connection states
    public enum ConnectionState {
        DISCONNECTED,
        CONNECTING,
        CONNECTED,
        CLOSING,
        CLOSED,
        ERROR
    }
    
    // Transport configuration
    public static class TransportConfig {
        public TransportType transportType;
        public int bufferSize;
        public int maxConnections;
        public long connectionTimeout;
        public long readTimeout;
        public long writeTimeout;
        public boolean enableKeepAlive;
        public boolean enableTcpNoDelay;
        public int soRcvBuf;
        public int soSndBuf;
        public int backlog;
        
        public TransportConfig() {
            this.transportType = TransportType.TCP;
            this.bufferSize = 8192;
            this.maxConnections = 1000;
            this.connectionTimeout = 30000;
            this.readTimeout = 60000;
            this.writeTimeout = 60000;
            this.enableKeepAlive = true;
            this.enableTcpNoDelay = true;
            this.soRcvBuf = 65536;
            this.soSndBuf = 65536;
            this.backlog = 128;
        }
    }
    
    // Network connection
    public static class NetworkConnection {
        public final long connectionId;
        public final SocketAddress remoteAddress;
        public final SocketAddress localAddress;
        public final TransportType transportType;
        public final long creationTime;
        public volatile ConnectionState state;
        public volatile long lastActivityTime;
        public volatile long bytesSent;
        public volatile long bytesReceived;
        public volatile int messagesSent;
        public volatile int messagesReceived;
        public final Map<String, Object> metadata;
        public final ReentrantLock connectionLock;
        
        public NetworkConnection(long connectionId, SocketAddress remoteAddress, 
                                SocketAddress localAddress, TransportType transportType) {
            this.connectionId = connectionId;
            this.remoteAddress = remoteAddress;
            this.localAddress = localAddress;
            this.transportType = transportType;
            this.creationTime = System.currentTimeMillis();
            this.state = ConnectionState.CONNECTING;
            this.lastActivityTime = creationTime;
            this.bytesSent = 0;
            this.bytesReceived = 0;
            this.messagesSent = 0;
            this.messagesReceived = 0;
            this.metadata = new ConcurrentHashMap<>();
            this.connectionLock = new ReentrantLock();
        }
        
        public long getAge() {
            return System.currentTimeMillis() - creationTime;
        }
        
        public long getIdleTime() {
            return System.currentTimeMillis() - lastActivityTime;
        }
        
        public void recordActivity() {
            lastActivityTime = System.currentTimeMillis();
        }
        
        public void recordBytesSent(int bytes) {
            bytesSent += bytes;
            recordActivity();
        }
        
        public void recordBytesReceived(int bytes) {
            bytesReceived += bytes;
            recordActivity();
        }
        
        public void recordMessageSent() {
            messagesSent++;
            recordActivity();
        }
        
        public void recordMessageReceived() {
            messagesReceived++;
            recordActivity();
        }
    }
    
    // Transport statistics
    public static class TransportStats {
        public final AtomicLong totalConnections;
        public final AtomicLong activeConnections;
        public final AtomicLong totalBytesSent;
        public final AtomicLong totalBytesReceived;
        public final AtomicLong totalMessagesSent;
        public final AtomicLong totalMessagesReceived;
        public final AtomicLong connectionErrors;
        public final AtomicLong readErrors;
        public final AtomicLong writeErrors;
        public final AtomicLong timeoutErrors;
        
        public TransportStats() {
            this.totalConnections = new AtomicLong(0);
            this.activeConnections = new AtomicLong(0);
            this.totalBytesSent = new AtomicLong(0);
            this.totalBytesReceived = new AtomicLong(0);
            this.totalMessagesSent = new AtomicLong(0);
            this.totalMessagesReceived = new AtomicLong(0);
            this.connectionErrors = new AtomicLong(0);
            this.readErrors = new AtomicLong(0);
            this.writeErrors = new AtomicLong(0);
            this.timeoutErrors = new AtomicLong(0);
        }
    }
    
    // Message callback interface
    public interface MessageCallback {
        void onMessageReceived(NetworkConnection connection, byte[] message);
        void onConnectionEstablished(NetworkConnection connection);
        void onConnectionClosed(NetworkConnection connection);
        void onError(NetworkConnection connection, Throwable error);
    }
    
    // Transport configuration
    private final TransportConfig config;
    
    // Selector for I/O multiplexing
    private Selector selector;
    
    // Connection management
    private final Map<Long, NetworkConnection> connections;
    private final Map<SocketAddress, NetworkConnection> addressToConnection;
    private final AtomicLong connectionIdGenerator;
    
    // Statistics
    private final TransportStats stats;
    
    // Thread pool for I/O operations
    private final ExecutorService ioExecutor;
    private final ScheduledExecutorService scheduledExecutor;
    
    // Message callback
    private volatile MessageCallback messageCallback;
    
    // Shutdown flag
    private volatile boolean shutdown;
    
    // Lock for connection management
    private final ReentrantLock connectionLock;
    
    /**
     * Constructor
     */
    public NetworkTransport(TransportConfig config) throws IOException {
        this.config = config;
        this.selector = Selector.open();
        this.connections = new ConcurrentHashMap<>();
        this.addressToConnection = new ConcurrentHashMap<>();
        this.connectionIdGenerator = new AtomicLong(0);
        this.stats = new TransportStats();
        this.ioExecutor = Executors.newFixedThreadPool(config.maxConnections);
        this.scheduledExecutor = Executors.newScheduledThreadPool(4);
        this.shutdown = false;
        this.connectionLock = new ReentrantLock();
        
        // Start I/O thread
        startIOThread();
        
        // Start cleanup thread
        startCleanupThread();
    }
    
    /**
     * Default constructor
     */
    public NetworkTransport() throws IOException {
        this(new TransportConfig());
    }
    
    /**
     * Set message callback
     */
    public void setMessageCallback(MessageCallback callback) {
        this.messageCallback = callback;
    }
    
    /**
     * Start I/O thread
     */
    private void startIOThread() {
        Thread ioThread = new Thread(() -> {
            while (!shutdown) {
                try {
                    selector.select(1000);
                    Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                    
                    while (keys.hasNext()) {
                        SelectionKey key = keys.next();
                        keys.remove();
                        
                        if (!key.isValid()) {
                            continue;
                        }
                        
                        if (key.isAcceptable()) {
                            handleAccept(key);
                        } else if (key.isReadable()) {
                            handleRead(key);
                        } else if (key.isWritable()) {
                            handleWrite(key);
                        } else if (key.isConnectable()) {
                            handleConnect(key);
                        }
                    }
                } catch (IOException e) {
                    if (!shutdown) {
                        e.printStackTrace();
                    }
                }
            }
        });
        ioThread.setName("NetworkTransport-IO");
        ioThread.setDaemon(true);
        ioThread.start();
    }
    
    /**
     * Start cleanup thread
     */
    private void startCleanupThread() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            cleanupIdleConnections();
            cleanupClosedConnections();
        }, 30000, 30000, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Connect to remote address
     */
    public NetworkConnection connect(String host, int port) throws IOException {
        return connect(new InetSocketAddress(host, port));
    }
    
    /**
     * Connect to remote address
     */
    public NetworkConnection connect(SocketAddress address) throws IOException {
        connectionLock.lock();
        try {
            // Check if connection already exists
            NetworkConnection existing = addressToConnection.get(address);
            if (existing != null && existing.state == ConnectionState.CONNECTED) {
                return existing;
            }
            
            // Create new connection
            long connectionId = connectionIdGenerator.incrementAndGet();
            SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(false);
            
            NetworkConnection connection = new NetworkConnection(
                connectionId, address, channel.getLocalAddress(), config.transportType
            );
            
            // Configure socket options
            configureSocket(channel);
            
            // Initiate connection
            channel.connect(address);
            channel.register(selector, SelectionKey.OP_CONNECT, connection);
            
            connections.put(connectionId, connection);
            addressToConnection.put(address, connection);
            stats.totalConnections.incrementAndGet();
            
            return connection;
        } finally {
            connectionLock.unlock();
        }
    }
    
    /**
     * Configure socket options
     */
    private void configureSocket(SocketChannel channel) throws IOException {
        if (config.transportType == TransportType.TCP) {
            if (config.enableKeepAlive) {
                channel.socket().setKeepAlive(true);
            }
            if (config.enableTcpNoDelay) {
                channel.socket().setTcpNoDelay(true);
            }
            if (config.soRcvBuf > 0) {
                channel.socket().setReceiveBufferSize(config.soRcvBuf);
            }
            if (config.soSndBuf > 0) {
                channel.socket().setSendBufferSize(config.soSndBuf);
            }
        }
    }
    
    /**
     * Handle accept operation (for server sockets)
     */
    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = serverChannel.accept();
        
        if (clientChannel != null) {
            clientChannel.configureBlocking(false);
            
            // Configure socket options
            configureSocket(clientChannel);
            
            // Create connection
            long connectionId = connectionIdGenerator.incrementAndGet();
            NetworkConnection connection = new NetworkConnection(
                connectionId, 
                clientChannel.getRemoteAddress(),
                clientChannel.getLocalAddress(),
                config.transportType
            );
            
            connection.state = ConnectionState.CONNECTED;
            connection.recordActivity();
            
            // Register for read operations
            clientChannel.register(selector, SelectionKey.OP_READ, connection);
            
            connections.put(connectionId, connection);
            addressToConnection.put(clientChannel.getRemoteAddress(), connection);
            stats.totalConnections.incrementAndGet();
            stats.activeConnections.incrementAndGet();
            
            // Notify callback
            if (messageCallback != null) {
                messageCallback.onConnectionEstablished(connection);
            }
        }
    }
    
    /**
     * Handle connection completion
     */
    private void handleConnect(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        NetworkConnection connection = (NetworkConnection) key.attachment();
        
        try {
            if (channel.finishConnect()) {
                connection.state = ConnectionState.CONNECTED;
                connection.recordActivity();
                stats.activeConnections.incrementAndGet();
                
                // Register for read operations
                channel.register(selector, SelectionKey.OP_READ, connection);
                
                // Notify callback
                if (messageCallback != null) {
                    messageCallback.onConnectionEstablished(connection);
                }
            }
        } catch (IOException e) {
            connection.state = ConnectionState.ERROR;
            stats.connectionErrors.incrementAndGet();
            closeConnection(connection);
            
            if (messageCallback != null) {
                messageCallback.onError(connection, e);
            }
        }
    }
    
    /**
     * Handle read operation
     */
    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        NetworkConnection connection = (NetworkConnection) key.attachment();
        
        ByteBuffer buffer = ByteBuffer.allocate(config.bufferSize);
        int bytesRead = 0;
        
        try {
            bytesRead = channel.read(buffer);
            
            if (bytesRead > 0) {
                buffer.flip();
                byte[] data = new byte[buffer.remaining()];
                buffer.get(data);
                
                connection.recordBytesReceived(bytesRead);
                connection.recordMessageReceived();
                stats.totalBytesReceived.addAndGet(bytesRead);
                stats.totalMessagesReceived.incrementAndGet();
                
                // Notify callback
                if (messageCallback != null) {
                    messageCallback.onMessageReceived(connection, data);
                }
            } else if (bytesRead < 0) {
                // Connection closed by remote
                closeConnection(connection);
            }
        } catch (IOException e) {
            connection.state = ConnectionState.ERROR;
            stats.readErrors.incrementAndGet();
            closeConnection(connection);
            
            if (messageCallback != null) {
                messageCallback.onError(connection, e);
            }
        }
    }
    
    /**
     * Handle write operation
     */
    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        NetworkConnection connection = (NetworkConnection) key.attachment();
        
        // Get pending data from connection metadata
        ByteBuffer pendingData = (ByteBuffer) connection.metadata.get("pendingWrite");
        
        if (pendingData != null) {
            try {
                int bytesWritten = channel.write(pendingData);
                connection.recordBytesSent(bytesWritten);
                stats.totalBytesSent.addAndGet(bytesWritten);
                
                if (!pendingData.hasRemaining()) {
                    // All data written, remove from metadata
                    connection.metadata.remove("pendingWrite");
                    // Switch back to read mode
                    channel.register(selector, SelectionKey.OP_READ, connection);
                }
            } catch (IOException e) {
                connection.state = ConnectionState.ERROR;
                stats.writeErrors.incrementAndGet();
                closeConnection(connection);
                
                if (messageCallback != null) {
                    messageCallback.onError(connection, e);
                }
            }
        }
    }
    
    /**
     * Send data to connection
     */
    public void send(NetworkConnection connection, byte[] data) throws IOException {
        if (connection.state != ConnectionState.CONNECTED) {
            throw new IOException("Connection not connected: " + connection.state);
        }
        
        connection.connectionLock.lock();
        try {
            SocketChannel channel = findChannelForConnection(connection);
            if (channel == null) {
                throw new IOException("Channel not found for connection: " + connection.connectionId);
            }
            
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int bytesWritten = channel.write(buffer);
            
            if (buffer.hasRemaining()) {
                // Data not fully written, register for write
                connection.metadata.put("pendingWrite", buffer);
                channel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, connection);
            } else {
                connection.recordBytesSent(bytesWritten);
                connection.recordMessageSent();
                stats.totalBytesSent.addAndGet(bytesWritten);
                stats.totalMessagesSent.incrementAndGet();
            }
        } finally {
            connection.connectionLock.unlock();
        }
    }
    
    /**
     * Find channel for connection
     */
    private SocketChannel findChannelForConnection(NetworkConnection connection) {
        for (SelectionKey key : selector.keys()) {
            if (key.attachment() == connection && key.channel() instanceof SocketChannel) {
                return (SocketChannel) key.channel();
            }
        }
        return null;
    }
    
    /**
     * Close connection
     */
    public void closeConnection(NetworkConnection connection) {
        connection.connectionLock.lock();
        try {
            if (connection.state == ConnectionState.CLOSED) {
                return;
            }
            
            connection.state = ConnectionState.CLOSING;
            
            SocketChannel channel = findChannelForConnection(connection);
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    // Ignore close errors
                }
            }
            
            connection.state = ConnectionState.CLOSED;
            stats.activeConnections.decrementAndGet();
            
            connections.remove(connection.connectionId);
            addressToConnection.remove(connection.remoteAddress);
            
            // Notify callback
            if (messageCallback != null) {
                messageCallback.onConnectionClosed(connection);
            }
        } finally {
            connection.connectionLock.unlock();
        }
    }
    
    /**
     * Cleanup idle connections
     */
    private void cleanupIdleConnections() {
        long now = System.currentTimeMillis();
        long idleTimeout = config.readTimeout * 2;
        
        for (NetworkConnection connection : connections.values()) {
            if (connection.state == ConnectionState.CONNECTED && 
                connection.getIdleTime() > idleTimeout) {
                closeConnection(connection);
            }
        }
    }
    
    /**
     * Cleanup closed connections
     */
    private void cleanupClosedConnections() {
        Iterator<Map.Entry<Long, NetworkConnection>> it = connections.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, NetworkConnection> entry = it.next();
            NetworkConnection connection = entry.getValue();
            
            if (connection.state == ConnectionState.CLOSED || 
                connection.state == ConnectionState.ERROR) {
                it.remove();
                addressToConnection.remove(connection.remoteAddress);
            }
        }
    }
    
    /**
     * Get statistics
     */
    public TransportStats getStats() {
        return stats;
    }
    
    /**
     * Get all connections
     */
    public Collection<NetworkConnection> getConnections() {
        return new ArrayList<>(connections.values());
    }
    
    /**
     * Get connection by ID
     */
    public NetworkConnection getConnection(long connectionId) {
        return connections.get(connectionId);
    }
    
    /**
     * Shutdown transport
     */
    public void shutdown() {
        shutdown = true;
        
        // Close all connections
        for (NetworkConnection connection : connections.values()) {
            closeConnection(connection);
        }
        
        // Close selector
        try {
            selector.close();
        } catch (IOException e) {
            // Ignore close errors
        }
        
        // Shutdown executors
        ioExecutor.shutdown();
        scheduledExecutor.shutdown();
        
        try {
            ioExecutor.awaitTermination(5, TimeUnit.SECONDS);
            scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
