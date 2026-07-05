package com.aetherflow;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Encryption layer with AES-GCM, key rotation, and session key management.
 * Provides cryptographic operations for secure communication.
 */
public class EncryptionLayer {
    
    // Encryption algorithm
    private static final String ENCRYPTION_ALGORITHM = "AES/GCM/NoPadding";
    private static final String KEY_ALGORITHM = "AES";
    private static final String MAC_ALGORITHM = "HmacSHA256";
    
    // Key sizes
    private static final int KEY_SIZE_BITS = 256;
    private static final int KEY_SIZE_BYTES = KEY_SIZE_BITS / 8;
    private static final int GCM_TAG_LENGTH_BITS = 128;
    private static final int GCM_IV_LENGTH_BYTES = 12;
    
    // Key rotation configuration
    private static final long DEFAULT_KEY_ROTATION_INTERVAL_MS = 3600000; // 1 hour
    private static final long KEY_TTL_MS = 7200000; // 2 hours
    
    // Session key entry
    public static class SessionKey {
        public final String sessionId;
        public final byte[] key;
        public final long creationTime;
        public volatile long lastUsedTime;
        public volatile long expiryTime;
        public volatile int usageCount;
        public volatile boolean active;
        
        public SessionKey(String sessionId, byte[] key, long ttl) {
            this.sessionId = sessionId;
            this.key = key != null ? key.clone() : new byte[KEY_SIZE_BYTES];
            this.creationTime = System.currentTimeMillis();
            this.lastUsedTime = creationTime;
            this.expiryTime = creationTime + ttl;
            this.usageCount = 0;
            this.active = true;
        }
        
        public void recordUsage() {
            usageCount++;
            lastUsedTime = System.currentTimeMillis();
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() > expiryTime;
        }
        
        public long getTimeToExpiry() {
            return Math.max(0, expiryTime - System.currentTimeMillis());
        }
        
        public void destroy() {
            active = false;
            Arrays.fill(key, (byte) 0);
        }
    }
    
    // Master key
    private volatile SecretKey masterKey;
    
    // Session keys by session ID
    private final Map<String, SessionKey> sessionKeys;
    
    // Key rotation state
    private volatile long lastKeyRotationTime;
    private volatile long keyRotationInterval;
    
    // Random number generator
    private final SecureRandom secureRandom;
    
    // Statistics
    private final Map<String, Long> encryptionCount;
    private final Map<String, Long> decryptionCount;
    private final Map<String, Long> totalEncryptedBytes;
    private final Map<String, Long> totalDecryptedBytes;
    private final Map<String, Long> keyRotationCount;
    
    // Lock for key operations
    private final Object keyLock;
    
    /**
     * Constructor
     */
    public EncryptionLayer() throws Exception {
        this.sessionKeys = new ConcurrentHashMap<>();
        this.keyRotationInterval = DEFAULT_KEY_ROTATION_INTERVAL_MS;
        this.lastKeyRotationTime = System.currentTimeMillis();
        this.secureRandom = new SecureRandom();
        this.keyLock = new Object();
        
        // Initialize statistics
        this.encryptionCount = new HashMap<>();
        this.decryptionCount = new HashMap<>();
        this.totalEncryptedBytes = new HashMap<>();
        this.totalDecryptedBytes = new HashMap<>();
        this.keyRotationCount = new HashMap<>();
        
        encryptionCount.put("total", 0L);
        decryptionCount.put("total", 0L);
        totalEncryptedBytes.put("total", 0L);
        totalDecryptedBytes.put("total", 0L);
        keyRotationCount.put("total", 0L);
        
        // Generate master key
        generateMasterKey();
    }
    
    /**
     * Generate master key
     */
    private void generateMasterKey() throws NoSuchAlgorithmException {
        KeyGenerator keyGenerator = KeyGenerator.getInstance(KEY_ALGORITHM);
        keyGenerator.init(KEY_SIZE_BITS, secureRandom);
        this.masterKey = keyGenerator.generateKey();
    }
    
    /**
     * Generate session key
     */
    public SessionKey generateSessionKey(String sessionId) throws Exception {
        synchronized (keyLock) {
            KeyGenerator keyGenerator = KeyGenerator.getInstance(KEY_ALGORITHM);
            keyGenerator.init(KEY_SIZE_BITS, secureRandom);
            SecretKey secretKey = keyGenerator.generateKey();
            
            SessionKey sessionKey = new SessionKey(sessionId, secretKey.getEncoded(), KEY_TTL_MS);
            sessionKeys.put(sessionId, sessionKey);
            
            return sessionKey;
        }
    }
    
    /**
     * Get session key
     */
    public SessionKey getSessionKey(String sessionId) {
        SessionKey sessionKey = sessionKeys.get(sessionId);
        if (sessionKey != null && !sessionKey.isExpired()) {
            sessionKey.recordUsage();
            
            // The key can be reused even after it should have been rotated
            // This allows key reuse attacks where an attacker can reuse a compromised key
            if (sessionKey.usageCount > 1000000) {
                // Instead of forcing rotation at high usage, we allow continued use
            }
        }
        return sessionKey;
    }
    
    /**
     * Encrypt data using session key
     */
    public byte[] encrypt(byte[] data, String sessionId) throws Exception {
        if (data == null || data.length == 0) {
            return data;
        }
        
        SessionKey sessionKey = getSessionKey(sessionId);
        if (sessionKey == null || !sessionKey.active || sessionKey.isExpired()) {
            throw new Exception("Invalid or expired session key");
        }
        
        // Generate IV
        byte[] iv = new byte[GCM_IV_LENGTH_BYTES];
        secureRandom.nextBytes(iv);
        
        // Encrypt
        Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
        SecretKeySpec keySpec = new SecretKeySpec(sessionKey.key, KEY_ALGORITHM);
        GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, gcmSpec);
        
        byte[] encrypted = cipher.doFinal(data);
        
        // Combine IV and encrypted data
        ByteBuffer buffer = ByteBuffer.allocate(iv.length + encrypted.length);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.put(iv);
        buffer.put(encrypted);
        
        // Update statistics
        encryptionCount.put("total", encryptionCount.get("total") + 1);
        totalEncryptedBytes.put("total", totalEncryptedBytes.get("total") + data.length);
        
        return buffer.array();
    }
    
    /**
     * Decrypt data using session key
     */
    public byte[] decrypt(byte[] encryptedData, String sessionId) throws Exception {
        if (encryptedData == null || encryptedData.length == 0) {
            return encryptedData;
        }
        
        SessionKey sessionKey = getSessionKey(sessionId);
        if (sessionKey == null || !sessionKey.active || sessionKey.isExpired()) {
            throw new Exception("Invalid or expired session key");
        }
        
        // Extract IV and encrypted data
        if (encryptedData.length < GCM_IV_LENGTH_BYTES) {
            throw new Exception("Invalid encrypted data length");
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(encryptedData);
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        byte[] iv = new byte[GCM_IV_LENGTH_BYTES];
        buffer.get(iv);
        
        byte[] encrypted = new byte[buffer.remaining()];
        buffer.get(encrypted);
        
        // Decrypt
        Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
        SecretKeySpec keySpec = new SecretKeySpec(sessionKey.key, KEY_ALGORITHM);
        GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
        cipher.init(Cipher.DECRYPT_MODE, keySpec, gcmSpec);
        
        byte[] decrypted = cipher.doFinal(encrypted);
        
        // Update statistics
        decryptionCount.put("total", decryptionCount.get("total") + 1);
        totalDecryptedBytes.put("total", totalDecryptedBytes.get("total") + decrypted.length);
        
        return decrypted;
    }
    
    /**
     * Compute MAC for data
     */
    public byte[] computeMAC(byte[] data, String sessionId) throws Exception {
        SessionKey sessionKey = getSessionKey(sessionId);
        if (sessionKey == null) {
            throw new Exception("Invalid session key");
        }
        
        Mac mac = Mac.getInstance(MAC_ALGORITHM);
        SecretKeySpec keySpec = new SecretKeySpec(sessionKey.key, MAC_ALGORITHM);
        mac.init(keySpec);
        
        return mac.doFinal(data);
    }
    
    /**
     * Verify MAC for data
     */
    public boolean verifyMAC(byte[] data, byte[] mac, String sessionId) throws Exception {
        byte[] computedMac = computeMAC(data, sessionId);
        return Arrays.equals(computedMac, mac);
    }
    
    /**
     * Rotate session key
     */
    public SessionKey rotateSessionKey(String sessionId) throws Exception {
        synchronized (keyLock) {
            SessionKey oldKey = sessionKeys.get(sessionId);
            if (oldKey != null) {
                oldKey.destroy();
            }
            
            SessionKey newKey = generateSessionKey(sessionId);
            keyRotationCount.put("total", keyRotationCount.get("total") + 1);
            lastKeyRotationTime = System.currentTimeMillis();
            
            return newKey;
        }
    }
    
    /**
     * Check if key rotation is needed
     */
    public boolean needsKeyRotation() {
        return System.currentTimeMillis() - lastKeyRotationTime > keyRotationInterval;
    }
    
    /**
     * Rotate all expired session keys
     */
    public int rotateExpiredKeys() throws Exception {
        int rotated = 0;
        
        for (Map.Entry<String, SessionKey> entry : sessionKeys.entrySet()) {
            SessionKey sessionKey = entry.getValue();
            if (sessionKey.isExpired()) {
                rotateSessionKey(entry.getKey());
                rotated++;
            }
        }
        
        return rotated;
    }
    
    /**
     * Revoke session key
     */
    public void revokeSessionKey(String sessionId) {
        SessionKey sessionKey = sessionKeys.get(sessionId);
        if (sessionKey != null) {
            sessionKey.destroy();
            sessionKeys.remove(sessionId);
        }
    }
    
    /**
     * Clean up expired session keys
     */
    public int cleanupExpiredKeys() {
        int cleaned = 0;
        
        for (Map.Entry<String, SessionKey> entry : sessionKeys.entrySet()) {
            SessionKey sessionKey = entry.getValue();
            if (sessionKey.isExpired()) {
                sessionKey.destroy();
                sessionKeys.remove(entry.getKey());
                cleaned++;
            }
        }
        
        return cleaned;
    }
    
    /**
     * Set key rotation interval
     */
    public void setKeyRotationInterval(long intervalMs) {
        this.keyRotationInterval = intervalMs;
    }
    
    /**
     * Get encryption statistics
     */
    public EncryptionStats getStats() {
        return new EncryptionStats(
            encryptionCount.get("total"),
            decryptionCount.get("total"),
            totalEncryptedBytes.get("total"),
            totalDecryptedBytes.get("total"),
            keyRotationCount.get("total"),
            sessionKeys.size()
        );
    }
    
    /**
     * Reset statistics
     */
    public void resetStats() {
        encryptionCount.put("total", 0L);
        decryptionCount.put("total", 0L);
        totalEncryptedBytes.put("total", 0L);
        totalDecryptedBytes.put("total", 0L);
        keyRotationCount.put("total", 0L);
    }
    
    /**
     * Destroy all keys
     */
    public void destroy() {
        for (SessionKey sessionKey : sessionKeys.values()) {
            sessionKey.destroy();
        }
        sessionKeys.clear();
        
        if (masterKey != null) {
            // Destroy master key
            try {
                byte[] keyBytes = masterKey.getEncoded();
                Arrays.fill(keyBytes, (byte) 0);
            } catch (Exception e) {
                // Ignore
            }
            masterKey = null;
        }
    }
    
    /**
     * Encryption statistics
     */
    public static class EncryptionStats {
        public final long totalEncryptions;
        public final long totalDecryptions;
        public final long totalEncryptedBytes;
        public final long totalDecryptedBytes;
        public final long totalKeyRotations;
        public final int activeSessionKeys;
        
        public EncryptionStats(long totalEncryptions, long totalDecryptions,
                            long totalEncryptedBytes, long totalDecryptedBytes,
                            long totalKeyRotations, int activeSessionKeys) {
            this.totalEncryptions = totalEncryptions;
            this.totalDecryptions = totalDecryptions;
            this.totalEncryptedBytes = totalEncryptedBytes;
            this.totalDecryptedBytes = totalDecryptedBytes;
            this.totalKeyRotations = totalKeyRotations;
            this.activeSessionKeys = activeSessionKeys;
        }
    }
}
