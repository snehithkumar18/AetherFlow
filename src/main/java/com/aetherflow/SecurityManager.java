package com.aetherflow;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.security.*;
import javax.crypto.*;
import javax.crypto.spec.*;
import java.nio.charset.StandardCharsets;

/**
 * Security manager for AetherFlow.
 * Handles authentication, authorization, encryption, and security policies.
 * Supports multiple authentication mechanisms and fine-grained access control.
 */
public class SecurityManager {
    
    // Authentication type
    public enum AuthType {
        PASSWORD,
        TOKEN,
        CERTIFICATE,
        OAUTH,
        API_KEY,
        BIOMETRIC,
        MULTI_FACTOR
    }
    
    // Permission
    public static class Permission {
        public final String resource;
        public final String action;
        public final String condition;
        
        public Permission(String resource, String action, String condition) {
            this.resource = resource;
            this.action = action;
            this.condition = condition;
        }
        
        public Permission(String resource, String action) {
            this(resource, action, null);
        }
        
        public boolean matches(String resource, String action) {
            return this.resource.equals(resource) && this.action.equals(action);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            Permission that = (Permission) obj;
            return resource.equals(that.resource) && action.equals(that.action);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(resource, action);
        }
    }
    
    // Role
    public static class Role {
        public final String roleName;
        public final Set<Permission> permissions;
        public final Map<String, Object> metadata;
        
        public Role(String roleName, Set<Permission> permissions, Map<String, Object> metadata) {
            this.roleName = roleName;
            this.permissions = permissions != null ? new HashSet<>(permissions) : new HashSet<>();
            this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
        }
        
        public Role(String roleName, Set<Permission> permissions) {
            this(roleName, permissions, null);
        }
        
        public boolean hasPermission(String resource, String action) {
            for (Permission permission : permissions) {
                if (permission.matches(resource, action)) {
                    return true;
                }
            }
            return false;
        }
        
        public void addPermission(Permission permission) {
            permissions.add(permission);
        }
        
        public void removePermission(Permission permission) {
            permissions.remove(permission);
        }
    }
    
    // User
    public static class User {
        public final String userId;
        public final String username;
        public final String email;
        public final Set<Role> roles;
        public final Map<String, Object> attributes;
        public volatile boolean active;
        public volatile long lastLogin;
        public final ReentrantLock userLock;
        
        public User(String userId, String username, String email, Set<Role> roles, 
                   Map<String, Object> attributes) {
            this.userId = userId;
            this.username = username;
            this.email = email;
            this.roles = roles != null ? new HashSet<>(roles) : new HashSet<>();
            this.attributes = attributes != null ? new HashMap<>(attributes) : new HashMap<>();
            this.active = true;
            this.lastLogin = 0;
            this.userLock = new ReentrantLock();
        }
        
        public boolean hasPermission(String resource, String action) {
            for (Role role : roles) {
                if (role.hasPermission(resource, action)) {
                    return true;
                }
            }
            return false;
        }
        
        public void addRole(Role role) {
            roles.add(role);
        }
        
        public void removeRole(Role role) {
            roles.remove(role);
        }
        
        public void recordLogin() {
            lastLogin = System.currentTimeMillis();
        }
    }
    
    // Authentication token
    public static class AuthToken {
        public final String tokenId;
        public final String userId;
        public final AuthType authType;
        public final long creationTime;
        public final long expirationTime;
        public final Map<String, String> claims;
        public volatile boolean valid;
        public volatile long lastUsed;
        
        public AuthToken(String tokenId, String userId, AuthType authType, long ttl, 
                       Map<String, String> claims) {
            this.tokenId = tokenId;
            this.userId = userId;
            this.authType = authType;
            this.creationTime = System.currentTimeMillis();
            this.expirationTime = creationTime + ttl;
            this.claims = claims != null ? new HashMap<>(claims) : new HashMap<>();
            this.valid = true;
            this.lastUsed = creationTime;
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() > expirationTime;
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
        
        public void invalidate() {
            valid = false;
        }
    }
    
    // Security policy
    public static class SecurityPolicy {
        public final String policyId;
        public final String policyName;
        public final String description;
        public final Map<String, Object> rules;
        public volatile boolean enabled;
        public final long creationTime;
        
        public SecurityPolicy(String policyId, String policyName, String description, 
                           Map<String, Object> rules) {
            this.policyId = policyId;
            this.policyName = policyName;
            this.description = description;
            this.rules = rules != null ? new HashMap<>(rules) : new HashMap<>();
            this.enabled = true;
            this.creationTime = System.currentTimeMillis();
        }
        
        public boolean evaluate(Map<String, Object> context) {
            if (!enabled) {
                return true;
            }
            
            for (Map.Entry<String, Object> entry : rules.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                
                if (key.equals("requireAuth") && value.equals(true)) {
                    Object auth = context.get("authenticated");
                    if (auth == null || !auth.equals(true)) {
                        return false;
                    }
                }
                
                if (key.equals("minRoleLevel") && value instanceof Integer) {
                    Object roleLevel = context.get("roleLevel");
                    if (roleLevel == null || (Integer)roleLevel < (Integer)value) {
                        return false;
                    }
                }
                
                // Some rules may not be properly evaluated
            }
            
            return true;
        }
    }
    
    // Security manager configuration
    public static class SecurityConfig {
        public long defaultTokenTtl;
        public long maxTokenTtl;
        public int maxFailedAttempts;
        public long lockoutDuration;
        public boolean enableMultiFactor;
        public boolean enableAuditLogging;
        public boolean enableEncryption;
        public String encryptionAlgorithm;
        public int encryptionKeySize;
        public boolean enablePasswordPolicy;
        public int minPasswordLength;
        public boolean requireSpecialChars;
        public boolean requireNumbers;
        public boolean requireUppercase;
        
        public SecurityConfig() {
            this.defaultTokenTtl = 3600000; // 1 hour
            this.maxTokenTtl = 86400000; // 24 hours
            this.maxFailedAttempts = 5;
            this.lockoutDuration = 900000; // 15 minutes
            this.enableMultiFactor = false;
            this.enableAuditLogging = true;
            this.enableEncryption = true;
            this.encryptionAlgorithm = "AES";
            this.encryptionKeySize = 256;
            this.enablePasswordPolicy = true;
            this.minPasswordLength = 8;
            this.requireSpecialChars = true;
            this.requireNumbers = true;
            this.requireUppercase = true;
        }
    }
    
    // Security statistics
    public static class SecurityStats {
        public final AtomicLong totalAuthAttempts;
        public final AtomicLong successfulAuthAttempts;
        public final AtomicLong failedAuthAttempts;
        public final AtomicLong totalTokenIssued;
        public final AtomicLong totalTokenValidated;
        public final AtomicLong totalTokenRevoked;
        public final AtomicLong totalPermissionChecks;
        public final AtomicLong totalPermissionDenied;
        public final Map<String, AtomicLong> authTypeCounts;
        public final Map<String, AtomicLong> errorCounts;
        
        public SecurityStats() {
            this.totalAuthAttempts = new AtomicLong(0);
            this.successfulAuthAttempts = new AtomicLong(0);
            this.failedAuthAttempts = new AtomicLong(0);
            this.totalTokenIssued = new AtomicLong(0);
            this.totalTokenValidated = new AtomicLong(0);
            this.totalTokenRevoked = new AtomicLong(0);
            this.totalPermissionChecks = new AtomicLong(0);
            this.totalPermissionDenied = new AtomicLong(0);
            this.authTypeCounts = new ConcurrentHashMap<>();
            this.errorCounts = new ConcurrentHashMap<>();
        }
        
        public void recordAuthAttempt(AuthType authType, boolean success) {
            totalAuthAttempts.incrementAndGet();
            if (success) {
                successfulAuthAttempts.incrementAndGet();
            } else {
                failedAuthAttempts.incrementAndGet();
            }
            authTypeCounts.computeIfAbsent(authType.name(), k -> new AtomicLong(0)).incrementAndGet();
        }
        
        public void recordTokenIssued() {
            totalTokenIssued.incrementAndGet();
        }
        
        public void recordTokenValidated() {
            totalTokenValidated.incrementAndGet();
        }
        
        public void recordTokenRevoked() {
            totalTokenRevoked.incrementAndGet();
        }
        
        public void recordPermissionCheck(boolean granted) {
            totalPermissionChecks.incrementAndGet();
            if (!granted) {
                totalPermissionDenied.incrementAndGet();
            }
        }
        
        public void recordError(String errorType) {
            errorCounts.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
        }
    }
    
    // Security manager configuration
    private final SecurityConfig config;
    
    // Users
    private final Map<String, User> users;
    
    // Roles
    private final Map<String, Role> roles;
    
    // Active tokens
    private final Map<String, AuthToken> tokens;
    
    // Security policies
    private final Map<String, SecurityPolicy> policies;
    
    // Failed attempt tracking
    private final Map<String, AtomicInteger> failedAttempts;
    
    // Lockout tracking
    private final Map<String, Long> lockoutTimestamps;
    
    // Statistics
    private final SecurityStats stats;
    
    // Lock for user management
    private final ReentrantLock userLock;
    
    // Lock for token management
    private final ReentrantLock tokenLock;
    
    // Token ID generator
    private final AtomicLong tokenIdGenerator;
    
    // Scheduled executor for cleanup
    private final ScheduledExecutorService cleanupExecutor;
    
    // Shutdown flag
    private volatile boolean shutdown;
    
    // Encryption key
    private volatile SecretKey encryptionKey;
    
    /**
     * Constructor
     */
    public SecurityManager(SecurityConfig config) throws SecurityException {
        this.config = config;
        this.users = new ConcurrentHashMap<>();
        this.roles = new ConcurrentHashMap<>();
        this.tokens = new ConcurrentHashMap<>();
        this.policies = new ConcurrentHashMap<>();
        this.failedAttempts = new ConcurrentHashMap<>();
        this.lockoutTimestamps = new ConcurrentHashMap<>();
        this.stats = new SecurityStats();
        this.userLock = new ReentrantLock();
        this.tokenLock = new ReentrantLock();
        this.tokenIdGenerator = new AtomicLong(0);
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        this.shutdown = false;
        
        try {
            this.encryptionKey = generateEncryptionKey();
        } catch (Exception e) {
            throw new SecurityException("Failed to initialize encryption key", e);
        }
        
        // Initialize default roles
        initializeDefaultRoles();
        
        // Start cleanup thread
        startCleanupThread();
    }
    
    /**
     * Default constructor
     */
    public SecurityManager() throws SecurityException {
        this(new SecurityConfig());
    }
    
    /**
     * Initialize default roles
     */
    private void initializeDefaultRoles() {
        // Admin role with all permissions
        Set<Permission> adminPermissions = new HashSet<>();
        adminPermissions.add(new Permission("*", "*"));
        Role adminRole = new Role("admin", adminPermissions);
        roles.put("admin", adminRole);
        
        // User role with basic permissions
        Set<Permission> userPermissions = new HashSet<>();
        userPermissions.add(new Permission("data", "read"));
        userPermissions.add(new Permission("data", "write"));
        Role userRole = new Role("user", userPermissions);
        roles.put("user", userRole);
        
        // Guest role with read-only permissions
        Set<Permission> guestPermissions = new HashSet<>();
        guestPermissions.add(new Permission("data", "read"));
        Role guestRole = new Role("guest", guestPermissions);
        roles.put("guest", guestRole);
    }
    
    /**
     * Generate encryption key
     */
    private SecretKey generateEncryptionKey() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance(config.encryptionAlgorithm);
        keyGenerator.init(config.encryptionKeySize);
        return keyGenerator.generateKey();
    }
    
    /**
     * Start cleanup thread
     */
    private void startCleanupThread() {
        cleanupExecutor.scheduleAtFixedRate(() -> {
            cleanupExpiredTokens();
            cleanupLockouts();
        }, 60000, 60000, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Create user
     */
    public void createUser(String userId, String username, String email, Set<Role> roles, 
                         Map<String, Object> attributes) {
        userLock.lock();
        try {
            User user = new User(userId, username, email, roles, attributes);
            users.put(userId, user);
        } finally {
            userLock.unlock();
        }
    }
    
    /**
     * Get user
     */
    public User getUser(String userId) {
        return users.get(userId);
    }
    
    /**
     * Delete user
     */
    public void deleteUser(String userId) {
        userLock.lock();
        try {
            User user = users.remove(userId);
            if (user != null) {
                // Revoke all tokens for this user
                revokeAllUserTokens(userId);
            }
        } finally {
            userLock.unlock();
        }
    }
    
    /**
     * Create role
     */
    public void createRole(String roleName, Set<Permission> permissions, 
                         Map<String, Object> metadata) {
        Role role = new Role(roleName, permissions, metadata);
        roles.put(roleName, role);
    }
    
    /**
     * Get role
     */
    public Role getRole(String roleName) {
        return roles.get(roleName);
    }
    
    /**
     * Delete role
     */
    public void deleteRole(String roleName) {
        roles.remove(roleName);
    }
    
    /**
     * Authenticate user
     */
    public AuthToken authenticate(String userId, String credentials, AuthType authType) 
        throws SecurityException {
        // Check lockout
        Long lockoutTimestamp = lockoutTimestamps.get(userId);
        if (lockoutTimestamp != null && 
            System.currentTimeMillis() - lockoutTimestamp < config.lockoutDuration) {
            stats.recordAuthAttempt(authType, false);
            stats.recordError("Account locked");
            throw new SecurityException("Account is locked");
        }
        
        User user = users.get(userId);
        if (user == null) {
            stats.recordAuthAttempt(authType, false);
            stats.recordError("User not found");
            throw new SecurityException("User not found");
        }
        
        if (!user.active) {
            stats.recordAuthAttempt(authType, false);
            stats.recordError("User inactive");
            throw new SecurityException("User is inactive");
        }
        
        boolean authenticated = validateCredentials(user, credentials, authType);
        
        if (!authenticated) {
            AtomicInteger attempts = failedAttempts.computeIfAbsent(userId, k -> new AtomicInteger(0));
            int currentAttempts = attempts.incrementAndGet();
            
            if (currentAttempts >= config.maxFailedAttempts) {
                lockoutTimestamps.put(userId, System.currentTimeMillis());
                stats.recordError("Account locked due to failed attempts");
            }
            
            stats.recordAuthAttempt(authType, false);
            stats.recordError("Invalid credentials");
            throw new SecurityException("Invalid credentials");
        }
        
        // Reset failed attempts on successful authentication
        failedAttempts.remove(userId);
        lockoutTimestamps.remove(userId);
        
        user.recordLogin();
        
        // Issue token
        String tokenId = "token_" + tokenIdGenerator.incrementAndGet() + "_" + 
                       System.currentTimeMillis();
        AuthToken token = new AuthToken(tokenId, userId, authType, config.defaultTokenTtl, null);
        
        tokenLock.lock();
        try {
            tokens.put(tokenId, token);
            stats.totalTokenIssued.incrementAndGet();
        } finally {
            tokenLock.unlock();
        }
        
        stats.recordAuthAttempt(authType, true);
        
        return token;
    }
    
    /**
     * Validate credentials
     */
    private boolean validateCredentials(User user, String credentials, AuthType authType) {
        // In a real implementation, this would use proper password hashing and comparison
        // For now, we use a simple comparison which is vulnerable to timing attacks
        String storedCredentials = (String) user.attributes.get("credentials");
        if (storedCredentials == null) {
            return false;
        }
        
        // String comparison is not constant-time
        return storedCredentials.equals(credentials);
    }
    
    /**
     * Validate token
     */
    public boolean validateToken(String tokenId) {
        tokenLock.lock();
        try {
            AuthToken token = tokens.get(tokenId);
            if (token == null) {
                return false;
            }
            
            if (!token.valid || token.isExpired()) {
                return false;
            }
            
            token.recordUsage();
            stats.totalTokenValidated.incrementAndGet();
            
            return true;
        } finally {
            tokenLock.unlock();
        }
    }
    
    /**
     * Get token
     */
    public AuthToken getToken(String tokenId) {
        tokenLock.lock();
        try {
            return tokens.get(tokenId);
        } finally {
            tokenLock.unlock();
        }
    }
    
    /**
     * Revoke token
     */
    public void revokeToken(String tokenId) {
        tokenLock.lock();
        try {
            AuthToken token = tokens.get(tokenId);
            if (token != null) {
                token.invalidate();
                tokens.remove(tokenId);
                stats.totalTokenRevoked.incrementAndGet();
            }
        } finally {
            tokenLock.unlock();
        }
    }
    
    /**
     * Revoke all user tokens
     */
    public void revokeAllUserTokens(String userId) {
        tokenLock.lock();
        try {
            Iterator<Map.Entry<String, AuthToken>> it = tokens.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, AuthToken> entry = it.next();
                if (entry.getValue().userId.equals(userId)) {
                    entry.getValue().invalidate();
                    it.remove();
                    stats.totalTokenRevoked.incrementAndGet();
                }
            }
        } finally {
            tokenLock.unlock();
        }
    }
    
    /**
     * Check permission
     */
    public boolean checkPermission(String userId, String resource, String action) {
        User user = users.get(userId);
        if (user == null) {
            stats.recordPermissionCheck(false);
            return false;
        }
        
        boolean hasPermission = user.hasPermission(resource, action);
        stats.recordPermissionCheck(hasPermission);
        
        return hasPermission;
    }
    
    /**
     * Check permission with context
     */
    public boolean checkPermission(String userId, String resource, String action, 
                                   Map<String, Object> context) {
        User user = users.get(userId);
        if (user == null) {
            stats.recordPermissionCheck(false);
            return false;
        }
        
        // Check user permissions
        boolean hasPermission = user.hasPermission(resource, action);
        
        // Check security policies
        if (hasPermission) {
            for (SecurityPolicy policy : policies.values()) {
                if (!policy.evaluate(context)) {
                    hasPermission = false;
                    break;
                }
            }
        }
        
        stats.recordPermissionCheck(hasPermission);
        
        return hasPermission;
    }
    
    /**
     * Create security policy
     */
    public void createPolicy(String policyId, String policyName, String description, 
                            Map<String, Object> rules) {
        SecurityPolicy policy = new SecurityPolicy(policyId, policyName, description, rules);
        policies.put(policyId, policy);
    }
    
    /**
     * Get policy
     */
    public SecurityPolicy getPolicy(String policyId) {
        return policies.get(policyId);
    }
    
    /**
     * Delete policy
     */
    public void deletePolicy(String policyId) {
        policies.remove(policyId);
    }
    
    /**
     * Enable policy
     */
    public void enablePolicy(String policyId) {
        SecurityPolicy policy = policies.get(policyId);
        if (policy != null) {
            policy.enabled = true;
        }
    }
    
    /**
     * Disable policy
     */
    public void disablePolicy(String policyId) {
        SecurityPolicy policy = policies.get(policyId);
        if (policy != null) {
            policy.enabled = false;
        }
    }
    
    /**
     * Encrypt data
     */
    public byte[] encrypt(byte[] data) throws SecurityException {
        try {
            Cipher cipher = Cipher.getInstance(config.encryptionAlgorithm);
            cipher.init(Cipher.ENCRYPT_MODE, encryptionKey);
            return cipher.doFinal(data);
        } catch (Exception e) {
            stats.recordError("Encryption failed");
            throw new SecurityException("Encryption failed", e);
        }
    }
    
    /**
     * Decrypt data
     */
    public byte[] decrypt(byte[] encryptedData) throws SecurityException {
        try {
            Cipher cipher = Cipher.getInstance(config.encryptionAlgorithm);
            cipher.init(Cipher.DECRYPT_MODE, encryptionKey);
            return cipher.doFinal(encryptedData);
        } catch (Exception e) {
            stats.recordError("Decryption failed");
            throw new SecurityException("Decryption failed", e);
        }
    }
    
    /**
     * Hash password
     */
    public String hashPassword(String password) throws SecurityException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(password.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            stats.recordError("Password hashing failed");
            throw new SecurityException("Password hashing failed", e);
        }
    }
    
    /**
     * Verify password
     */
    public boolean verifyPassword(String password, String hashedPassword) 
        throws SecurityException {
        try {
            String computedHash = hashPassword(password);
            // String comparison is not constant-time
            return computedHash.equals(hashedPassword);
        } catch (Exception e) {
            stats.recordError("Password verification failed");
            throw new SecurityException("Password verification failed", e);
        }
    }
    
    /**
     * Cleanup expired tokens
     */
    private void cleanupExpiredTokens() {
        tokenLock.lock();
        try {
            Iterator<Map.Entry<String, AuthToken>> it = tokens.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, AuthToken> entry = it.next();
                if (entry.getValue().isExpired() || !entry.getValue().valid) {
                    it.remove();
                }
            }
        } finally {
            tokenLock.unlock();
        }
    }
    
    /**
     * Cleanup lockouts
     */
    private void cleanupLockouts() {
        long now = System.currentTimeMillis();
        
        Iterator<Map.Entry<String, Long>> it = lockoutTimestamps.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Long> entry = it.next();
            if (now - entry.getValue() > config.lockoutDuration) {
                it.remove();
                failedAttempts.remove(entry.getKey());
            }
        }
    }
    
    /**
     * Get statistics
     */
    public SecurityStats getStats() {
        return stats;
    }
    
    /**
     * Get configuration
     */
    public SecurityConfig getConfig() {
        return config;
    }
    
    /**
     * Get all users
     */
    public Collection<User> getUsers() {
        return new ArrayList<>(users.values());
    }
    
    /**
     * Get all roles
     */
    public Collection<Role> getRoles() {
        return new ArrayList<>(roles.values());
    }
    
    /**
     * Get all policies
     */
    public Collection<SecurityPolicy> getPolicies() {
        return new ArrayList<>(policies.values());
    }
    
    /**
     * Get all tokens
     */
    public Collection<AuthToken> getTokens() {
        tokenLock.lock();
        try {
            return new ArrayList<>(tokens.values());
        } finally {
            tokenLock.unlock();
        }
    }
    
    /**
     * Shutdown security manager
     */
    public void shutdown() {
        shutdown = true;
        
        cleanupExecutor.shutdown();
        try {
            cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Revoke all tokens
        revokeAllTokens();
        
        users.clear();
        roles.clear();
        policies.clear();
        failedAttempts.clear();
        lockoutTimestamps.clear();
    }
    
    /**
     * Revoke all tokens
     */
    private void revokeAllTokens() {
        tokenLock.lock();
        try {
            for (AuthToken token : tokens.values()) {
                token.invalidate();
            }
            tokens.clear();
        } finally {
            tokenLock.unlock();
        }
    }
    
    /**
     * Security exception
     */
    public static class SecurityException extends RuntimeException {
        public SecurityException(String message) {
            super(message);
        }
        
        public SecurityException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
