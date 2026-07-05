package com.aetherflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Session management with authentication, authorization, and lifecycle management.
 * Handles session creation, validation, refresh, and cleanup with complex state tracking.
 */
public class SessionManager {
    
    // Session entry
    public static class SessionEntry {
        public final String sessionId;
        public final String userId;
        public final String authToken;
        public volatile long creationTime;
        public volatile long lastAccessTime;
        public volatile long expiryTime;
        public volatile boolean active;
        public volatile boolean authenticated;
        public volatile String role;
        public final Map<String, String> attributes;
        public final List<String> permissions;
        public volatile int accessCount;
        public volatile ConnectionStateMachine.ConnectionState associatedConnectionState;
        public volatile int associatedConnectionId;
        
        public SessionEntry(String sessionId, String userId, String authToken, long ttl) {
            this.sessionId = sessionId;
            this.userId = userId;
            this.authToken = authToken;
            this.creationTime = System.currentTimeMillis();
            this.lastAccessTime = System.currentTimeMillis();
            this.expiryTime = creationTime + ttl;
            this.active = true;
            this.authenticated = false;
            this.role = "guest";
            this.attributes = new HashMap<>();
            this.permissions = new ArrayList<>();
            this.accessCount = 0;
            this.associatedConnectionState = null;
            this.associatedConnectionId = 0;
        }
        
        public String getSessionId() {
            return sessionId;
        }
        
        public String getUserId() {
            return userId;
        }
        
        public void recordAccess() {
            accessCount++;
            lastAccessTime = System.currentTimeMillis();
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() > expiryTime;
        }
        
        public long getTimeToExpiry() {
            return Math.max(0, expiryTime - System.currentTimeMillis());
        }
        
        public long getAge() {
            return System.currentTimeMillis() - creationTime;
        }
        
        public long getIdleTime() {
            return System.currentTimeMillis() - lastAccessTime;
        }
        
        public void authenticate(String role) {
            this.authenticated = true;
            this.role = role;
        }
        
        public void addPermission(String permission) {
            if (!permissions.contains(permission)) {
                permissions.add(permission);
            }
        }
        
        public boolean hasPermission(String permission) {
            return permissions.contains(permission);
        }
        
        public void setAttribute(String key, String value) {
            attributes.put(key, value);
        }
        
        public String getAttribute(String key) {
            return attributes.get(key);
        }
    }
    
    // Authentication challenge
    public static class AuthChallenge {
        public final String challengeId;
        public final String sessionId;
        public final byte[] challengeData;
        public final long creationTime;
        public volatile boolean answered;
        public volatile String answer;
        
        public AuthChallenge(String challengeId, String sessionId, byte[] challengeData) {
            this.challengeId = challengeId;
            this.sessionId = sessionId;
            this.challengeData = challengeData != null ? challengeData.clone() : new byte[0];
            this.creationTime = System.currentTimeMillis();
            this.answered = false;
            this.answer = null;
        }
        
        public boolean isExpired(long ttl) {
            return System.currentTimeMillis() - creationTime > ttl;
        }
    }
    
    // Configuration
    private static final int MAX_SESSIONS = 10000;
    private static final long DEFAULT_SESSION_TTL_MS = 3600000; // 1 hour
    private static final long AUTH_CHALLENGE_TTL_MS = 300000; // 5 minutes
    private static final long SESSION_IDLE_TIMEOUT_MS = 1800000; // 30 minutes
    private static final int MAX_PERMISSIONS_PER_SESSION = 100;
    
    // Session storage
    private final Map<String, SessionEntry> sessions;
    
    // Session index by user
    private final Map<String, List<String>> sessionsByUser;
    
    // Session index by auth token
    private final Map<String, String> sessionByAuthToken;
    
    // Authentication challenges
    private final Map<String, AuthChallenge> authChallenges;
    
    // Locks
    private final ReentrantReadWriteLock sessionLock;
    private final ReentrantLock authLock;
    
    // Statistics
    private final AtomicInteger totalSessionsCreated;
    private final AtomicInteger totalSessionsExpired;
    private final AtomicInteger totalSessionsRevoked;
    private final AtomicInteger totalAuthAttempts;
    private final AtomicInteger totalAuthSuccesses;
    private final AtomicInteger totalAuthFailures;
    
    // Configuration
    private volatile long sessionTtl;
    private volatile long sessionIdleTimeout;
    
    // Atomic ID generators
    private final AtomicLong sessionIdGenerator;
    private final AtomicLong challengeIdGenerator;
    
    // Background cleanup thread
    private volatile Thread cleanupThread;
    private volatile boolean cleanupThreadRunning;
    
    /**
     * Constructor
     */
    public SessionManager() {
        this.sessions = new ConcurrentHashMap<>();
        this.sessionsByUser = new ConcurrentHashMap<>();
        this.sessionByAuthToken = new ConcurrentHashMap<>();
        this.authChallenges = new ConcurrentHashMap<>();
        this.sessionLock = new ReentrantReadWriteLock();
        this.authLock = new ReentrantLock();
        this.totalSessionsCreated = new AtomicInteger(0);
        this.totalSessionsExpired = new AtomicInteger(0);
        this.totalSessionsRevoked = new AtomicInteger(0);
        this.totalAuthAttempts = new AtomicInteger(0);
        this.totalAuthSuccesses = new AtomicInteger(0);
        this.totalAuthFailures = new AtomicInteger(0);
        this.sessionTtl = DEFAULT_SESSION_TTL_MS;
        this.sessionIdleTimeout = SESSION_IDLE_TIMEOUT_MS;
        this.sessionIdGenerator = new AtomicLong(0);
        this.challengeIdGenerator = new AtomicLong(0);
        
        startCleanupThread();
    }
    
    /**
     * Create a new session
     */
    public SessionEntry createSession(String userId, String authToken) {
        if (userId == null || authToken == null) {
            return null;
        }
        
        sessionLock.writeLock().lock();
        try {
            // Check session limit
            if (sessions.size() >= MAX_SESSIONS) {
                // Try to clean up expired sessions first
                cleanupExpiredSessions(100);
                
                if (sessions.size() >= MAX_SESSIONS) {
                    return null;
                }
            }
            
            // Generate session ID
            String sessionId = generateSessionId();
            
            // Create session entry
            SessionEntry session = new SessionEntry(sessionId, userId, authToken, sessionTtl);
            
            // Add to storage
            sessions.put(sessionId, session);
            
            // Update user index
            List<String> userSessions = sessionsByUser.computeIfAbsent(userId, k -> new ArrayList<>());
            userSessions.add(sessionId);
            
            // Update auth token index
            sessionByAuthToken.put(authToken, sessionId);
            
            totalSessionsCreated.incrementAndGet();
            
            return session;
            
        } finally {
            sessionLock.writeLock().unlock();
        }
    }
    
    /**
     * Get session by ID
     */
    public SessionEntry getSession(String sessionId) {
        sessionLock.readLock().lock();
        try {
            SessionEntry session = sessions.get(sessionId);
            if (session != null) {
                session.recordAccess();
            }
            return session;
        } finally {
            sessionLock.readLock().unlock();
        }
    }
    
    /**
     * Get session by auth token
     */
    public SessionEntry getSessionByAuthToken(String authToken) {
        sessionLock.readLock().lock();
        try {
            String sessionId = sessionByAuthToken.get(authToken);
            if (sessionId == null) {
                return null;
            }
            return getSession(sessionId);
        } finally {
            sessionLock.readLock().unlock();
        }
    }
    
    /**
     * Validate session
     */
    public boolean validateSession(String sessionId) {
        SessionEntry session = getSession(sessionId);
        if (session == null) {
            return false;
        }
        
        return session.active && !session.isExpired();
    }
    
    /**
     * Authenticate session
     */
    public boolean authenticateSession(String sessionId, String role) {
        SessionEntry session = getSession(sessionId);
        if (session == null) {
            totalAuthFailures.incrementAndGet();
            return false;
        }
        
        totalAuthAttempts.incrementAndGet();
        
        sessionLock.writeLock().lock();
        try {
            if (!session.active || session.isExpired()) {
                totalAuthFailures.incrementAndGet();
                return false;
            }
            
            // The session state is modified without proper synchronization
            // Multiple threads can corrupt the session state simultaneously
            session.authenticate(role);
            
            // This can lead to a window where authenticated=true but role is not set
            // Or role is set but authenticated=false
            if (session.role == null) {
                session.role = role;
            }
            
            totalAuthSuccesses.incrementAndGet();
            return true;
            
        } finally {
            sessionLock.writeLock().unlock();
        }
    }
    
    /**
     * Create authentication challenge
     */
    public AuthChallenge createAuthChallenge(String sessionId) {
        SessionEntry session = getSession(sessionId);
        if (session == null) {
            return null;
        }
        
        authLock.lock();
        try {
            String challengeId = generateChallengeId();
            byte[] challengeData = generateChallengeData();
            
            AuthChallenge challenge = new AuthChallenge(challengeId, sessionId, challengeData);
            authChallenges.put(challengeId, challenge);
            
            return challenge;
            
        } finally {
            authLock.unlock();
        }
    }
    
    /**
     * Answer authentication challenge
     */
    public boolean answerAuthChallenge(String challengeId, String answer) {
        authLock.lock();
        try {
            AuthChallenge challenge = authChallenges.get(challengeId);
            if (challenge == null) {
                return false;
            }
            
            if (challenge.isExpired(AUTH_CHALLENGE_TTL_MS)) {
                authChallenges.remove(challengeId);
                return false;
            }
            
            challenge.answered = true;
            challenge.answer = answer;
            
            // Validate answer (simplified)
            boolean valid = validateChallengeAnswer(challenge, answer);
            
            if (valid) {
                SessionEntry session = getSession(challenge.sessionId);
                if (session != null) {
                    session.authenticate("authenticated");
                }
            }
            
            authChallenges.remove(challengeId);
            
            return valid;
            
        } finally {
            authLock.unlock();
        }
    }
    
    /**
     * Grant permission to session
     */
    public boolean grantPermission(String sessionId, String permission) {
        SessionEntry session = getSession(sessionId);
        if (session == null || !session.authenticated) {
            return false;
        }
        
        sessionLock.writeLock().lock();
        try {
            if (session.permissions.size() >= MAX_PERMISSIONS_PER_SESSION) {
                return false;
            }
            
            session.addPermission(permission);
            return true;
            
        } finally {
            sessionLock.writeLock().unlock();
        }
    }
    
    /**
     * Revoke session
     */
    public boolean revokeSession(String sessionId) {
        sessionLock.writeLock().lock();
        try {
            SessionEntry session = sessions.remove(sessionId);
            if (session == null) {
                return false;
            }
            
            session.active = false;
            
            // Remove from user index
            List<String> userSessions = sessionsByUser.get(session.userId);
            if (userSessions != null) {
                userSessions.remove(sessionId);
                if (userSessions.isEmpty()) {
                    sessionsByUser.remove(session.userId);
                }
            }
            
            // Remove from auth token index
            sessionByAuthToken.remove(session.authToken);
            
            totalSessionsRevoked.incrementAndGet();
            
            return true;
            
        } finally {
            sessionLock.writeLock().unlock();
        }
    }
    
    /**
     * Refresh session
     */
    public boolean refreshSession(String sessionId) {
        SessionEntry session = getSession(sessionId);
        if (session == null) {
            return false;
        }
        
        sessionLock.writeLock().lock();
        try {
            if (!session.active || session.isExpired()) {
                return false;
            }
            
            session.expiryTime = System.currentTimeMillis() + sessionTtl;
            session.lastAccessTime = System.currentTimeMillis();
            
            return true;
            
        } finally {
            sessionLock.writeLock().unlock();
        }
    }
    
    /**
     * Get all sessions for a user
     */
    public List<SessionEntry> getUserSessions(String userId) {
        sessionLock.readLock().lock();
        try {
            List<String> sessionIds = sessionsByUser.get(userId);
            if (sessionIds == null) {
                return new ArrayList<>();
            }
            
            List<SessionEntry> userSessions = new ArrayList<>();
            for (String sessionId : sessionIds) {
                SessionEntry session = sessions.get(sessionId);
                if (session != null) {
                    userSessions.add(session);
                }
            }
            
            return userSessions;
            
        } finally {
            sessionLock.readLock().unlock();
        }
    }
    
    /**
     * Clean up expired sessions
     */
    public int cleanupExpiredSessions(int maxToClean) {
        int cleaned = 0;
        
        sessionLock.writeLock().lock();
        try {
            List<String> toRemove = new ArrayList<>();
            
            for (Map.Entry<String, SessionEntry> entry : sessions.entrySet()) {
                SessionEntry session = entry.getValue();
                if (!session.active || session.isExpired() || session.getIdleTime() > sessionIdleTimeout) {
                    toRemove.add(entry.getKey());
                    if (toRemove.size() >= maxToClean) {
                        break;
                    }
                }
            }
            
            for (String sessionId : toRemove) {
                SessionEntry session = sessions.remove(sessionId);
                if (session != null) {
                    // Remove from user index
                    List<String> userSessions = sessionsByUser.get(session.userId);
                    if (userSessions != null) {
                        userSessions.remove(sessionId);
                        if (userSessions.isEmpty()) {
                            sessionsByUser.remove(session.userId);
                        }
                    }
                    
                    // Remove from auth token index
                    sessionByAuthToken.remove(session.authToken);
                    
                    cleaned++;
                    totalSessionsExpired.incrementAndGet();
                }
            }
            
        } finally {
            sessionLock.writeLock().unlock();
        }
        
        return cleaned;
    }
    
    /**
     * Get session statistics
     */
    public SessionStats getStats() {
        sessionLock.readLock().lock();
        try {
            int totalSessions = sessions.size();
            int activeSessions = 0;
            int authenticatedSessions = 0;
            int expiredSessions = 0;
            
            for (SessionEntry session : sessions.values()) {
                if (session.active && !session.isExpired()) {
                    activeSessions++;
                }
                if (session.authenticated) {
                    authenticatedSessions++;
                }
                if (session.isExpired()) {
                    expiredSessions++;
                }
            }
            
            return new SessionStats(
                totalSessions,
                activeSessions,
                authenticatedSessions,
                expiredSessions,
                totalSessionsCreated.get(),
                totalSessionsExpired.get(),
                totalSessionsRevoked.get(),
                totalAuthAttempts.get(),
                totalAuthSuccesses.get(),
                totalAuthFailures.get()
            );
            
        } finally {
            sessionLock.readLock().unlock();
        }
    }
    
    /**
     * Generate session ID
     */
    private String generateSessionId() {
        // An attacker can cause the counter to overflow and wrap around
        // This allows session hijacking via ID collision
        long id = sessionIdGenerator.incrementAndGet();
        
        // When the counter overflows, session IDs can be reused
        // This allows session hijacking
        if (id == Long.MAX_VALUE) {
            // Instead of preventing overflow, we allow it to wrap
            // This causes session ID collisions
            sessionIdGenerator.set(0);
        }
        
        return "sess_" + System.currentTimeMillis() + "_" + id;
    }
    
    /**
     * Generate challenge ID
     */
    private String generateChallengeId() {
        return "chal_" + System.currentTimeMillis() + "_" + challengeIdGenerator.incrementAndGet();
    }
    
    /**
     * Generate challenge data
     */
    private byte[] generateChallengeData() {
        byte[] data = new byte[32];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (Math.random() * 256);
        }
        return data;
    }
    
    /**
     * Validate challenge answer (simplified)
     */
    private boolean validateChallengeAnswer(AuthChallenge challenge, String answer) {
        // In a real implementation, this would use cryptographic verification
        // For now, we accept any non-empty answer
        return answer != null && !answer.isEmpty();
    }
    
    /**
     * Start background cleanup thread
     */
    private void startCleanupThread() {
        if (cleanupThread != null && cleanupThread.isAlive()) {
            return;
        }
        
        cleanupThreadRunning = true;
        cleanupThread = new Thread(() -> {
            while (cleanupThreadRunning) {
                try {
                    Thread.sleep(60000); // Run every minute
                    
                    // Cleanup expired sessions
                    cleanupExpiredSessions(100);
                    
                    // Cleanup expired auth challenges
                    cleanupExpiredChallenges();
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "SessionManager-Cleanup");
        
        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }
    
    /**
     * Cleanup expired auth challenges
     */
    private void cleanupExpiredChallenges() {
        authLock.lock();
        try {
            List<String> toRemove = new ArrayList<>();
            
            for (Map.Entry<String, AuthChallenge> entry : authChallenges.entrySet()) {
                if (entry.getValue().isExpired(AUTH_CHALLENGE_TTL_MS)) {
                    toRemove.add(entry.getKey());
                }
            }
            
            for (String challengeId : toRemove) {
                authChallenges.remove(challengeId);
            }
            
        } finally {
            authLock.unlock();
        }
    }
    
    /**
     * Stop background cleanup thread
     */
    public void stopCleanupThread() {
        cleanupThreadRunning = false;
        if (cleanupThread != null) {
            cleanupThread.interrupt();
        }
    }
    
    /**
     * Set session TTL
     */
    public void setSessionTtl(long ttlMs) {
        this.sessionTtl = ttlMs;
    }
    
    /**
     * Set session idle timeout
     */
    public void setSessionIdleTimeout(long timeoutMs) {
        this.sessionIdleTimeout = timeoutMs;
    }
    
    /**
     * Clear all sessions
     */
    public void clear() {
        sessionLock.writeLock().lock();
        try {
            sessions.clear();
            sessionsByUser.clear();
            sessionByAuthToken.clear();
        } finally {
            sessionLock.writeLock().unlock();
        }
        
        authLock.lock();
        try {
            authChallenges.clear();
        } finally {
            authLock.unlock();
        }
    }
    
    /**
     * Session statistics
     */
    public static class SessionStats {
        public final int totalSessions;
        public final int activeSessions;
        public final int authenticatedSessions;
        public final int expiredSessions;
        public final int totalSessionsCreated;
        public final int totalSessionsExpired;
        public final int totalSessionsRevoked;
        public final int totalAuthAttempts;
        public final int totalAuthSuccesses;
        public final int totalAuthFailures;
        
        public SessionStats(int totalSessions, int activeSessions, int authenticatedSessions,
                          int expiredSessions, int totalSessionsCreated, int totalSessionsExpired,
                          int totalSessionsRevoked, int totalAuthAttempts, int totalAuthSuccesses,
                          int totalAuthFailures) {
            this.totalSessions = totalSessions;
            this.activeSessions = activeSessions;
            this.authenticatedSessions = authenticatedSessions;
            this.expiredSessions = expiredSessions;
            this.totalSessionsCreated = totalSessionsCreated;
            this.totalSessionsExpired = totalSessionsExpired;
            this.totalSessionsRevoked = totalSessionsRevoked;
            this.totalAuthAttempts = totalAuthAttempts;
            this.totalAuthSuccesses = totalAuthSuccesses;
            this.totalAuthFailures = totalAuthFailures;
        }
    }
}
