package com.aetherflow;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.CRC32;

/**
 * Multi-layer protocol validation for AetherFlow messages.
 * Provides checksums, MAC validation, magic byte verification, and format validation.
 */
public class ProtocolValidator {
    
    // Validation levels
    public enum ValidationLevel {
        NONE,
        BASIC,
        STANDARD,
        STRICT,
        PARANOID
    }
    
    // Current validation level
    private ValidationLevel validationLevel;
    
    // Enable/disable specific validations
    private boolean validateMagicBytes;
    private boolean validateChecksum;
    private boolean validateMAC;
    private boolean validateVersion;
    private boolean validatePayloadSize;
    private boolean validateTimestamp;
    private boolean validateSequence;
    
    // MAC key for message authentication
    private byte[] macKey;
    
    // Maximum payload size
    private static final int MAX_PAYLOAD_SIZE = 16 * 1024 * 1024; // 16MB
    private static final int MIN_PAYLOAD_SIZE = 0;
    
    // Timestamp tolerance in seconds (5 minutes)
    private static final int TIMESTAMP_TOLERANCE = 300;
    
    // CRC32 instance for checksum calculation
    private final CRC32 crc32;
    
    /**
     * Constructor with default validation level
     */
    public ProtocolValidator() {
        this(ValidationLevel.STANDARD);
    }
    
    /**
     * Constructor with specified validation level
     */
    public ProtocolValidator(ValidationLevel validationLevel) {
        this.validationLevel = validationLevel;
        this.crc32 = new CRC32();
        this.macKey = new byte[32]; // Default 256-bit key
        configureValidationLevel();
    }
    
    /**
     * Configure validation settings based on level
     */
    private void configureValidationLevel() {
        switch (validationLevel) {
            case NONE:
                validateMagicBytes = false;
                validateChecksum = false;
                validateMAC = false;
                validateVersion = false;
                validatePayloadSize = false;
                validateTimestamp = false;
                validateSequence = false;
                break;
            case BASIC:
                validateMagicBytes = true;
                validateChecksum = false;
                validateMAC = false;
                validateVersion = true;
                validatePayloadSize = true;
                validateTimestamp = false;
                validateSequence = false;
                break;
            case STANDARD:
                validateMagicBytes = true;
                validateChecksum = true;
                validateMAC = false;
                validateVersion = true;
                validatePayloadSize = true;
                validateTimestamp = true;
                validateSequence = false;
                break;
            case STRICT:
                validateMagicBytes = true;
                validateChecksum = true;
                validateMAC = true;
                validateVersion = true;
                validatePayloadSize = true;
                validateTimestamp = true;
                validateSequence = true;
                break;
            case PARANOID:
                validateMagicBytes = true;
                validateChecksum = true;
                validateMAC = true;
                validateVersion = true;
                validatePayloadSize = true;
                validateTimestamp = true;
                validateSequence = true;
                break;
        }
    }
    
    /**
     * Set validation level
     */
    public void setValidationLevel(ValidationLevel level) {
        this.validationLevel = level;
        configureValidationLevel();
    }
    
    /**
     * Set MAC key for message authentication
     */
    public void setMacKey(byte[] key) {
        this.macKey = key != null ? key.clone() : new byte[32];
    }
    
    /**
     * Validate a protocol message
     */
    public ValidationResult validate(ProtocolMessage message) {
        ValidationResult result = new ValidationResult();
        
        if (message == null) {
            result.valid = false;
            result.errors.add("Message is null");
            return result;
        }
        
        // Validate magic bytes
        if (validateMagicBytes) {
            if (!validateMagicBytes(message)) {
                result.valid = false;
                result.errors.add("Invalid magic bytes");
            }
        }
        
        // Validate protocol version
        if (validateVersion) {
            if (!validateVersion(message)) {
                result.valid = false;
                result.errors.add("Invalid protocol version");
            }
        }
        
        // Validate payload size
        if (validatePayloadSize) {
            if (!validatePayloadSize(message)) {
                result.valid = false;
                result.errors.add("Invalid payload size");
            }
        }
        
        // Validate checksum
        if (validateChecksum) {
            if (!validateChecksum(message)) {
                result.valid = false;
                result.errors.add("Invalid checksum");
            }
        }
        
        // Validate MAC
        if (validateMAC) {
            if (!validateMAC(message)) {
                result.valid = false;
                result.errors.add("Invalid MAC");
            }
        }
        
        // Validate timestamp
        if (validateTimestamp) {
            if (!validateTimestamp(message)) {
                result.valid = false;
                result.errors.add("Invalid timestamp");
            }
        }
        
        // Validate sequence number
        if (validateSequence) {
            if (!validateSequence(message)) {
                result.valid = false;
                result.errors.add("Invalid sequence number");
            }
        }
        
        // Validate message type
        if (!validateMessageType(message)) {
            result.valid = false;
            result.errors.add("Invalid message type");
        }
        
        // Validate fragment fields if fragmentation is enabled
        if (message.isFragmentationEnabled()) {
            if (!validateFragmentFields(message)) {
                result.valid = false;
                result.errors.add("Invalid fragment fields");
            }
        }
        
        return result;
    }
    
    /**
     * Validate magic bytes
     */
    private boolean validateMagicBytes(ProtocolMessage message) {
        byte[] magic = message.getMagic();
        if (magic == null || magic.length != ProtocolMessage.MAGIC_BYTES.length) {
            return false;
        }
        return Arrays.equals(magic, ProtocolMessage.MAGIC_BYTES);
    }
    
    /**
     * Validate protocol version
     */
    private boolean validateVersion(ProtocolMessage message) {
        int version = message.getProtocolVersion();
        // Accept current version and one minor version back
        return version == ProtocolMessage.PROTOCOL_VERSION || 
               version == (ProtocolMessage.PROTOCOL_VERSION - 1);
    }
    
    /**
     * Validate payload size
     */
    private boolean validatePayloadSize(ProtocolMessage message) {
        int payloadLength = message.getPayloadLength();
        return payloadLength >= MIN_PAYLOAD_SIZE && payloadLength <= MAX_PAYLOAD_SIZE;
    }
    
    /**
     * Validate checksum using CRC32
     */
    private boolean validateChecksum(ProtocolMessage message) {
        int expectedChecksum = message.getChecksum();
        if (expectedChecksum == 0) {
            // Checksum not set, skip validation
            return true;
        }
        
        int calculatedChecksum = calculateChecksum(message);
        
        if (calculatedChecksum != expectedChecksum) {
            return false;
        }
        
        return true;
    }
    
    /**
     * Calculate checksum for a message
     */
    private int calculateChecksum(ProtocolMessage message) {
        crc32.reset();
        
        // Add magic bytes
        crc32.update(message.getMagic());
        
        // Add protocol version
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putShort((short) message.getProtocolVersion());
        crc32.update(buffer.array());
        
        // Add message type
        if (message.getMessageType() != null) {
            crc32.update(message.getMessageType().getCode());
        }
        
        // Add sequence number
        buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(message.getSequenceNumber());
        crc32.update(buffer.array());
        
        // Add session ID
        buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(message.getSessionId());
        crc32.update(buffer.array());
        
        // Add flags
        buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(message.getFlags());
        crc32.update(buffer.array());
        
        // Add payload length
        buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(message.getPayloadLength());
        crc32.update(buffer.array());
        
        // Add timestamp
        buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(message.getTimestamp());
        crc32.update(buffer.array());
        
        // Add payload
        crc32.update(message.getPayload());
        
        return (int) crc32.getValue();
    }
    
    /**
     * Validate MAC (Message Authentication Code)
     * Simplified HMAC-like implementation
     */
    private boolean validateMAC(ProtocolMessage message) {
        if (macKey == null || macKey.length == 0) {
            return true; // No MAC key set, skip validation
        }
        
        int expectedMAC = message.getChecksum(); // Reuse checksum field for MAC
        if (expectedMAC == 0) {
            return true; // MAC not set
        }
        
        int calculatedMAC = calculateMAC(message);
        return expectedMAC == calculatedMAC;
    }
    
    /**
     * Calculate MAC for a message
     */
    private int calculateMAC(ProtocolMessage message) {
        int mac = 0;
        
        // XOR-based MAC with key
        byte[] data = message.serialize();
        for (int i = 0; i < data.length; i++) {
            mac ^= (data[i] & 0xFF) * (macKey[i % macKey.length] & 0xFF);
        }
        
        return mac & 0xFFFFFFFF;
    }
    
    /**
     * Validate timestamp
     */
    private boolean validateTimestamp(ProtocolMessage message) {
        int timestamp = message.getTimestamp();
        int currentTime = (int) (System.currentTimeMillis() / 1000);
        
        // Allow messages from the past and future within tolerance
        int diff = Math.abs(timestamp - currentTime);
        return diff <= TIMESTAMP_TOLERANCE;
    }
    
    /**
     * Validate sequence number
     */
    private boolean validateSequence(ProtocolMessage message) {
        int sequenceNumber = message.getSequenceNumber();
        // Sequence number should be non-negative
        return sequenceNumber >= 0;
    }
    
    /**
     * Validate message type
     */
    private boolean validateMessageType(ProtocolMessage message) {
        return message.getMessageType() != null;
    }
    
    /**
     * Validate fragment fields
     */
    private boolean validateFragmentFields(ProtocolMessage message) {
        int fragmentIndex = message.getFragmentIndex();
        int totalFragments = message.getTotalFragments();
        int fragmentOffset = message.getFragmentOffset();
        int totalFragmentSize = message.getTotalFragmentSize();
        
        // Validate fragment index
        if (fragmentIndex < 0 || fragmentIndex >= totalFragments) {
            return false;
        }
        
        // Validate total fragments
        if (totalFragments <= 0 || totalFragments > 1000) {
            return false;
        }
        
        // Validate fragment offset
        if (fragmentOffset < 0 || fragmentOffset >= totalFragmentSize) {
            return false;
        }
        
        // Validate total fragment size
        if (totalFragmentSize <= 0 || totalFragmentSize > MAX_PAYLOAD_SIZE) {
            return false;
        }
        
        return true;
    }
    
    /**
     * Validate raw byte data as a protocol message
     */
    public ValidationResult validateBytes(byte[] data) {
        ValidationResult result = new ValidationResult();
        
        if (data == null || data.length < 35) {
            result.valid = false;
            result.errors.add("Data too short");
            return result;
        }
        
        // Check magic bytes
        if (validateMagicBytes) {
            if (data.length < 4 || 
                data[0] != ProtocolMessage.MAGIC_BYTES[0] ||
                data[1] != ProtocolMessage.MAGIC_BYTES[1] ||
                data[2] != ProtocolMessage.MAGIC_BYTES[2] ||
                data[3] != ProtocolMessage.MAGIC_BYTES[3]) {
                result.valid = false;
                result.errors.add("Invalid magic bytes");
                return result;
            }
        }
        
        // Try to deserialize and validate
        try {
            ProtocolMessage message = ProtocolMessage.deserialize(data);
            return validate(message);
        } catch (Exception e) {
            result.valid = false;
            result.errors.add("Deserialization failed: " + e.getMessage());
            return result;
        }
    }
    
    /**
     * Validation result
     */
    public static class ValidationResult {
        public boolean valid;
        public final java.util.List<String> errors;
        
        public ValidationResult() {
            this.valid = true;
            this.errors = new java.util.ArrayList<>();
        }
    }
}
