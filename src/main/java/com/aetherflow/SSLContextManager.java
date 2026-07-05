package com.aetherflow;

import javax.net.ssl.*;
import java.io.*;
import java.security.*;
import java.security.cert.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SSL/TLS context manager for AetherFlow.
 * Manages SSL contexts, certificate validation, and secure communication.
 * Supports multiple SSL protocols, cipher suites, and certificate management.
 */
public class SSLContextManager {
    
    // SSL protocol versions
    public enum SSLProtocol {
        TLSv1_2("TLSv1.2"),
        TLSv1_3("TLSv1.3");
        
        private final String protocolName;
        
        SSLProtocol(String protocolName) {
            this.protocolName = protocolName;
        }
        
        public String getProtocolName() {
            return protocolName;
        }
    }
    
    // SSL configuration
    public static class SSLConfig {
        public SSLProtocol protocol;
        public String[] enabledCipherSuites;
        public String[] enabledProtocols;
        public boolean clientAuth;
        public boolean hostnameVerification;
        public int sessionTimeout;
        public int sessionCacheSize;
        public boolean enableSessionResumption;
        public String trustStorePath;
        public String trustStorePassword;
        public String keyStorePath;
        public String keyStorePassword;
        public String keyPassword;
        
        public SSLConfig() {
            this.protocol = SSLProtocol.TLSv1_3;
            this.enabledCipherSuites = null; // Use JVM defaults
            this.enabledProtocols = null; // Use JVM defaults
            this.clientAuth = false;
            this.hostnameVerification = true;
            this.sessionTimeout = 300; // 5 minutes
            this.sessionCacheSize = 1000;
            this.enableSessionResumption = true;
            this.trustStorePath = null;
            this.trustStorePassword = null;
            this.keyStorePath = null;
            this.keyStorePassword = null;
            this.keyPassword = null;
        }
    }
    
    // SSL session info
    public static class SSLSessionInfo {
        public final String sessionId;
        public final long creationTime;
        public final long lastAccessedTime;
        public final long timeout;
        public final String protocol;
        public final String cipherSuite;
        public final String peerHost;
        public final int peerPort;
        public volatile boolean valid;
        
        public SSLSessionInfo(String sessionId, SSLSession session, String peerHost, int peerPort) {
            this.sessionId = sessionId;
            this.creationTime = session.getCreationTime();
            this.lastAccessedTime = session.getLastAccessedTime();
            this.timeout = 300; // Default 5 minutes
            this.protocol = session.getProtocol();
            this.cipherSuite = session.getCipherSuite();
            this.peerHost = peerHost;
            this.peerPort = peerPort;
            this.valid = true;
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() - lastAccessedTime > timeout * 1000;
        }
    }
    
    // SSL statistics
    public static class SSLStats {
        public final AtomicLong totalHandshakes;
        public final AtomicLong successfulHandshakes;
        public final AtomicLong failedHandshakes;
        public final AtomicLong sessionResumptions;
        public final AtomicLong sessionCreations;
        public final AtomicLong certificateErrors;
        public final AtomicLong verificationErrors;
        public final AtomicLong activeSessions;
        
        public SSLStats() {
            this.totalHandshakes = new AtomicLong(0);
            this.successfulHandshakes = new AtomicLong(0);
            this.failedHandshakes = new AtomicLong(0);
            this.sessionResumptions = new AtomicLong(0);
            this.sessionCreations = new AtomicLong(0);
            this.certificateErrors = new AtomicLong(0);
            this.verificationErrors = new AtomicLong(0);
            this.activeSessions = new AtomicLong(0);
        }
    }
    
    // SSL context cache
    private final Map<String, SSLContext> contextCache;
    private final Map<String, SSLSessionInfo> sessionCache;
    
    // SSL configuration
    private final SSLConfig config;
    
    // Statistics
    private final SSLStats stats;
    
    // Lock for thread safety
    private final ReentrantLock contextLock;
    private final ReentrantLock sessionLock;
    
    // Scheduled executor for cleanup
    private final ScheduledExecutorService cleanupExecutor;
    
    // Trust manager
    private volatile X509TrustManager trustManager;
    
    // Key manager
    private volatile X509KeyManager keyManager;
    
    /**
     * Constructor
     */
    public SSLContextManager(SSLConfig config) {
        this.config = config;
        this.contextCache = new ConcurrentHashMap<>();
        this.sessionCache = new ConcurrentHashMap<>();
        this.stats = new SSLStats();
        this.contextLock = new ReentrantLock();
        this.sessionLock = new ReentrantLock();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        
        // Initialize trust and key managers
        initializeManagers();
        
        // Start cleanup thread
        startCleanupThread();
    }
    
    /**
     * Default constructor
     */
    public SSLContextManager() {
        this(new SSLConfig());
    }
    
    /**
     * Initialize trust and key managers
     */
    private void initializeManagers() {
        try {
            // Initialize trust manager
            TrustManagerFactory trustManagerFactory;
            if (config.trustStorePath != null) {
                KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                try (InputStream trustStoreStream = new FileInputStream(config.trustStorePath)) {
                    trustStore.load(trustStoreStream, 
                        config.trustStorePassword != null ? 
                        config.trustStorePassword.toCharArray() : null);
                }
                trustManagerFactory = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(trustStore);
            } else {
                trustManagerFactory = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init((KeyStore) null);
            }
            
            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            for (TrustManager tm : trustManagers) {
                if (tm instanceof X509TrustManager) {
                    this.trustManager = (X509TrustManager) tm;
                    break;
                }
            }
            
            // Initialize key manager
            if (config.keyStorePath != null) {
                KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                try (InputStream keyStoreStream = new FileInputStream(config.keyStorePath)) {
                    keyStore.load(keyStoreStream, 
                        config.keyStorePassword != null ? 
                        config.keyStorePassword.toCharArray() : null);
                }
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
                    KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, 
                    config.keyPassword != null ? 
                    config.keyPassword.toCharArray() : null);
                
                KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();
                for (KeyManager km : keyManagers) {
                    if (km instanceof X509KeyManager) {
                        this.keyManager = (X509KeyManager) km;
                        break;
                    }
                }
            }
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize SSL managers", e);
        }
    }
    
    /**
     * Start cleanup thread
     */
    private void startCleanupThread() {
        cleanupExecutor.scheduleAtFixedRate(() -> {
            cleanupExpiredSessions();
        }, 60000, 60000, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Get SSL context for given host and port
     */
    public SSLContext getSSLContext(String host, int port) throws SSLException {
        String contextKey = host + ":" + port;
        
        SSLContext context = contextCache.get(contextKey);
        if (context != null) {
            return context;
        }
        
        contextLock.lock();
        try {
            // Double-check after acquiring lock
            context = contextCache.get(contextKey);
            if (context != null) {
                return context;
            }
            
            // Create new SSL context
            context = createSSLContext(host, port);
            contextCache.put(contextKey, context);
            
            return context;
        } finally {
            contextLock.unlock();
        }
    }
    
    /**
     * Create SSL context
     */
    private SSLContext createSSLContext(String host, int port) throws SSLException {
        try {
            SSLContext context = SSLContext.getInstance(config.protocol.getProtocolName());
            
            // Create custom trust manager if hostname verification is enabled
            TrustManager[] trustManagers;
            if (config.hostnameVerification && trustManager != null) {
                trustManagers = new TrustManager[] {
                    new HostnameVerifyingTrustManager(trustManager, host)
                };
            } else {
                trustManagers = new TrustManager[] { trustManager };
            }
            
            // Create key manager array
            KeyManager[] keyManagers = keyManager != null ? 
                new KeyManager[] { keyManager } : null;
            
            context.init(keyManagers, trustManagers, new SecureRandom());
            
            // Configure SSL parameters
            SSLParameters sslParams = context.getDefaultSSLParameters();
            if (config.enabledCipherSuites != null) {
                sslParams.setCipherSuites(config.enabledCipherSuites);
            }
            if (config.enabledProtocols != null) {
                sslParams.setProtocols(config.enabledProtocols);
            }
            if (config.clientAuth) {
                sslParams.setNeedClientAuth(true);
            }
            
            return context;
        } catch (Exception e) {
            throw new SSLException("Failed to create SSL context", e);
        }
    }
    
    /**
     * Create SSL engine
     */
    public SSLEngine createSSLEngine(String host, int port) throws SSLException {
        SSLContext context = getSSLContext(host, port);
        SSLEngine engine = context.createSSLEngine(host, port);
        engine.setUseClientMode(true);
        
        // Configure engine
        SSLParameters sslParams = engine.getSSLParameters();
        if (config.enabledCipherSuites != null) {
            sslParams.setCipherSuites(config.enabledCipherSuites);
        }
        if (config.enabledProtocols != null) {
            sslParams.setProtocols(config.enabledProtocols);
        }
        if (config.hostnameVerification) {
            sslParams.setEndpointIdentificationAlgorithm("HTTPS");
        }
        engine.setSSLParameters(sslParams);
        
        return engine;
    }
    
    /**
     * Record SSL session
     */
    public void recordSession(SSLEngine engine, String host, int port) {
        if (!config.enableSessionResumption) {
            return;
        }
        
        SSLSession session = engine.getSession();
        String sessionId = bytesToHex(session.getId());
        
        sessionLock.lock();
        try {
            SSLSessionInfo sessionInfo = new SSLSessionInfo(sessionId, session, host, port);
            sessionCache.put(sessionId, sessionInfo);
            stats.activeSessions.incrementAndGet();
            
            // Check if this is a resumed session
            if (session.isValid()) {
                stats.sessionResumptions.incrementAndGet();
            } else {
                stats.sessionCreations.incrementAndGet();
            }
        } finally {
            sessionLock.unlock();
        }
    }
    
    /**
     * Get SSL session info
     */
    public SSLSessionInfo getSessionInfo(String sessionId) {
        sessionLock.lock();
        try {
            return sessionCache.get(sessionId);
        } finally {
            sessionLock.unlock();
        }
    }
    
    /**
     * Cleanup expired sessions
     */
    private void cleanupExpiredSessions() {
        sessionLock.lock();
        try {
            Iterator<Map.Entry<String, SSLSessionInfo>> it = sessionCache.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, SSLSessionInfo> entry = it.next();
                SSLSessionInfo sessionInfo = entry.getValue();
                
                if (sessionInfo.isExpired() || !sessionInfo.valid) {
                    it.remove();
                    stats.activeSessions.decrementAndGet();
                }
            }
        } finally {
            sessionLock.unlock();
        }
    }
    
    /**
     * Record handshake start
     */
    public void recordHandshakeStart() {
        stats.totalHandshakes.incrementAndGet();
    }
    
    /**
     * Record handshake success
     */
    public void recordHandshakeSuccess() {
        stats.successfulHandshakes.incrementAndGet();
    }
    
    /**
     * Record handshake failure
     */
    public void recordHandshakeFailure() {
        stats.failedHandshakes.incrementAndGet();
    }
    
    /**
     * Record certificate error
     */
    public void recordCertificateError() {
        stats.certificateErrors.incrementAndGet();
    }
    
    /**
     * Record verification error
     */
    public void recordVerificationError() {
        stats.verificationErrors.incrementAndGet();
    }
    
    /**
     * Get statistics
     */
    public SSLStats getStats() {
        return stats;
    }
    
    /**
     * Get configuration
     */
    public SSLConfig getConfig() {
        return config;
    }
    
    /**
     * Clear context cache
     */
    public void clearContextCache() {
        contextLock.lock();
        try {
            contextCache.clear();
        } finally {
            contextLock.unlock();
        }
    }
    
    /**
     * Clear session cache
     */
    public void clearSessionCache() {
        sessionLock.lock();
        try {
            sessionCache.clear();
            stats.activeSessions.set(0);
        } finally {
            sessionLock.unlock();
        }
    }
    
    /**
     * Shutdown manager
     */
    public void shutdown() {
        cleanupExecutor.shutdown();
        try {
            cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        clearContextCache();
        clearSessionCache();
    }
    
    /**
     * Convert bytes to hex string
     */
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
    
    /**
     * Hostname verifying trust manager
     */
    private static class HostnameVerifyingTrustManager implements X509TrustManager {
        private final X509TrustManager delegate;
        private final String expectedHostname;
        
        public HostnameVerifyingTrustManager(X509TrustManager delegate, String expectedHostname) {
            this.delegate = delegate;
            this.expectedHostname = expectedHostname;
        }
        
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) 
            throws CertificateException {
            delegate.checkClientTrusted(chain, authType);
        }
        
        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) 
            throws CertificateException {
            delegate.checkServerTrusted(chain, authType);
            
            // Verify hostname
            if (expectedHostname != null && chain.length > 0) {
                try {
                    verifyHostname(chain[0], expectedHostname);
                } catch (CertificateException e) {
                    throw e;
                }
            }
        }
        
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return delegate.getAcceptedIssuers();
        }
        
        /**
         * Verify hostname
         */
        private void verifyHostname(X509Certificate cert, String hostname) 
            throws CertificateException {
            String cn = getCommonName(cert);
            if (cn != null && cn.equals(hostname)) {
                return;
            }
            
            if (cn != null && cn.startsWith("*.") && 
                hostname.endsWith(cn.substring(1))) {
                return;
            }
            
            throw new CertificateException("Hostname verification failed");
        }
        
        /**
         * Get common name from certificate
         */
        private String getCommonName(X509Certificate cert) throws CertificateException {
            String dn = cert.getSubjectX500Principal().getName();
            String[] parts = dn.split(",");
            for (String part : parts) {
                part = part.trim();
                if (part.startsWith("CN=")) {
                    return part.substring(3);
                }
            }
            return null;
        }
    }
}
