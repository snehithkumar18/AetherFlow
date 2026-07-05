package com.aetherflow;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Core protocol message structure for AetherFlow binary communication.
 * Supports multiple message types with variant payloads and complex header validation.
 */
public class ProtocolMessage {
    // Message type enumeration
    public enum MessageType {
        HANDSHAKE(0x01),
        AUTH(0x02),
        DATA(0x03),
        ACK(0x04),
        NACK(0x05),
        HEARTBEAT(0x06),
        SESSION_CREATE(0x07),
        SESSION_CLOSE(0x08),
        KEY_ROTATION(0x09),
        COMPRESSED_DATA(0x0A),
        ENCRYPTED_DATA(0x0B),
        FRAGMENT_START(0x0C),
        FRAGMENT_CONT(0x0D),
        FRAGMENT_END(0x0E),
        RETRANSMIT_REQUEST(0x0F),
        FLOW_CONTROL(0x10),
        ERROR(0x11),
        VARIANT(0x12),
        BATCH(0x13);
        
        private final byte code;
        
        MessageType(int code) {
            this.code = (byte) code;
        }
        
        public byte getCode() {
            return code;
        }
        
        public static MessageType fromCode(byte code) {
            for (MessageType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            return null;
        }
    }
    
    // Protocol version
    public static final short PROTOCOL_VERSION_MAJOR = 2;
    public static final short PROTOCOL_VERSION_MINOR = 1;
    public static final int PROTOCOL_VERSION = (PROTOCOL_VERSION_MAJOR << 8) | PROTOCOL_VERSION_MINOR;
    
    // Magic bytes for protocol identification
    public static final byte[] MAGIC_BYTES = new byte[] { 0x4E, 0x58, 0x50, 0x52 }; // "NXPR"
    
    // Header fields
    private byte[] magic;
    private int protocolVersion;
    private MessageType messageType;
    private int sequenceNumber;
    private int sessionId;
    private int flags;
    private int payloadLength;
    private int checksum;
    private int timestamp;
    private byte[] payload;
    
    // Feature flags
    private boolean compressionEnabled;
    private boolean encryptionEnabled;
    private boolean fragmentationEnabled;
    private boolean retransmissionEnabled;
    
    // Extended header fields
    private Map<String, String> extendedHeaders;
    private int fragmentOffset;
    private int totalFragmentSize;
    private int fragmentIndex;
    private int totalFragments;
    
    /**
     * Default constructor
     */
    public ProtocolMessage() {
        this.magic = MAGIC_BYTES.clone();
        this.protocolVersion = PROTOCOL_VERSION;
        this.sequenceNumber = 0;
        this.sessionId = 0;
        this.flags = 0;
        this.payloadLength = 0;
        this.checksum = 0;
        this.timestamp = (int) (System.currentTimeMillis() / 1000);
        this.payload = new byte[0];
        this.extendedHeaders = new HashMap<>();
        this.fragmentOffset = 0;
        this.totalFragmentSize = 0;
        this.fragmentIndex = 0;
        this.totalFragments = 0;
    }
    
    /**
     * Constructor with message type
     */
    public ProtocolMessage(MessageType messageType) {
        this();
        this.messageType = messageType;
    }
    
    /**
     * Constructor with message type and payload
     */
    public ProtocolMessage(MessageType messageType, byte[] payload) {
        this(messageType);
        this.payload = payload != null ? payload.clone() : new byte[0];
        this.payloadLength = this.payload.length;
    }
    
    // Getters and Setters
    public byte[] getMagic() {
        return magic.clone();
    }
    
    public void setMagic(byte[] magic) {
        this.magic = magic != null ? magic.clone() : MAGIC_BYTES.clone();
    }
    
    public int getProtocolVersion() {
        return protocolVersion;
    }
    
    public void setProtocolVersion(int protocolVersion) {
        this.protocolVersion = protocolVersion;
    }
    
    public MessageType getMessageType() {
        return messageType;
    }
    
    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }
    
    public int getSequenceNumber() {
        return sequenceNumber;
    }
    
    public void setSequenceNumber(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }
    
    public int getSessionId() {
        return sessionId;
    }
    
    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }
    
    public int getFlags() {
        return flags;
    }
    
    public void setFlags(int flags) {
        this.flags = flags;
    }
    
    public int getPayloadLength() {
        return payloadLength;
    }
    
    public void setPayloadLength(int payloadLength) {
        this.payloadLength = payloadLength;
    }
    
    public int getChecksum() {
        return checksum;
    }
    
    public void setChecksum(int checksum) {
        this.checksum = checksum;
    }
    
    public int getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }
    
    public byte[] getPayload() {
        return payload.clone();
    }
    
    public void setPayload(byte[] payload) {
        this.payload = payload != null ? payload.clone() : new byte[0];
        this.payloadLength = this.payload.length;
    }
    
    public boolean isCompressionEnabled() {
        return compressionEnabled;
    }
    
    public void setCompressionEnabled(boolean compressionEnabled) {
        this.compressionEnabled = compressionEnabled;
        updateFlags();
    }
    
    public boolean isEncryptionEnabled() {
        return encryptionEnabled;
    }
    
    public void setEncryptionEnabled(boolean encryptionEnabled) {
        this.encryptionEnabled = encryptionEnabled;
        updateFlags();
    }
    
    public boolean isFragmentationEnabled() {
        return fragmentationEnabled;
    }
    
    public void setFragmentationEnabled(boolean fragmentationEnabled) {
        this.fragmentationEnabled = fragmentationEnabled;
        updateFlags();
    }
    
    public boolean isRetransmissionEnabled() {
        return retransmissionEnabled;
    }
    
    public void setRetransmissionEnabled(boolean retransmissionEnabled) {
        this.retransmissionEnabled = retransmissionEnabled;
        updateFlags();
    }
    
    public Map<String, String> getExtendedHeaders() {
        return new HashMap<>(extendedHeaders);
    }
    
    public void setExtendedHeaders(Map<String, String> extendedHeaders) {
        this.extendedHeaders = extendedHeaders != null ? new HashMap<>(extendedHeaders) : new HashMap<>();
    }
    
    public void addExtendedHeader(String key, String value) {
        this.extendedHeaders.put(key, value);
    }
    
    public String getExtendedHeader(String key) {
        return this.extendedHeaders.get(key);
    }
    
    public int getFragmentOffset() {
        return fragmentOffset;
    }
    
    public void setFragmentOffset(int fragmentOffset) {
        this.fragmentOffset = fragmentOffset;
    }
    
    public int getTotalFragmentSize() {
        return totalFragmentSize;
    }
    
    public void setTotalFragmentSize(int totalFragmentSize) {
        this.totalFragmentSize = totalFragmentSize;
    }
    
    public int getFragmentIndex() {
        return fragmentIndex;
    }
    
    public void setFragmentIndex(int fragmentIndex) {
        this.fragmentIndex = fragmentIndex;
    }
    
    public int getTotalFragments() {
        return totalFragments;
    }
    
    public void setTotalFragments(int totalFragments) {
        this.totalFragments = totalFragments;
    }
    
    /**
     * Update flags based on feature settings
     */
    private void updateFlags() {
        int newFlags = 0;
        if (compressionEnabled) {
            newFlags |= 0x01;
        }
        if (encryptionEnabled) {
            newFlags |= 0x02;
        }
        if (fragmentationEnabled) {
            newFlags |= 0x04;
        }
        if (retransmissionEnabled) {
            newFlags |= 0x08;
        }
        this.flags = newFlags;
    }
    
    /**
     * Parse flags into feature settings
     */
    public void parseFlags() {
        this.compressionEnabled = (flags & 0x01) != 0;
        this.encryptionEnabled = (flags & 0x02) != 0;
        this.fragmentationEnabled = (flags & 0x04) != 0;
        this.retransmissionEnabled = (flags & 0x08) != 0;
    }
    
    /**
     * Calculate checksum for the message
     */
    public int calculateChecksum() {
        int sum = 0;
        
        // Add magic bytes
        for (byte b : magic) {
            sum += (b & 0xFF);
        }
        
        // Add protocol version
        sum += (protocolVersion >> 8) & 0xFF;
        sum += protocolVersion & 0xFF;
        
        // Add message type
        if (messageType != null) {
            sum += messageType.getCode() & 0xFF;
        }
        
        // Add sequence number
        sum += (sequenceNumber >> 24) & 0xFF;
        sum += (sequenceNumber >> 16) & 0xFF;
        sum += (sequenceNumber >> 8) & 0xFF;
        sum += sequenceNumber & 0xFF;
        
        // Add session ID
        sum += (sessionId >> 24) & 0xFF;
        sum += (sessionId >> 16) & 0xFF;
        sum += (sessionId >> 8) & 0xFF;
        sum += sessionId & 0xFF;
        
        // Add flags
        sum += (flags >> 24) & 0xFF;
        sum += (flags >> 16) & 0xFF;
        sum += (flags >> 8) & 0xFF;
        sum += flags & 0xFF;
        
        // Add payload length
        sum += (payloadLength >> 24) & 0xFF;
        sum += (payloadLength >> 16) & 0xFF;
        sum += (payloadLength >> 8) & 0xFF;
        sum += payloadLength & 0xFF;
        
        // Add timestamp
        sum += (timestamp >> 24) & 0xFF;
        sum += (timestamp >> 16) & 0xFF;
        sum += (timestamp >> 8) & 0xFF;
        sum += timestamp & 0xFF;
        
        // Add payload bytes
        for (byte b : payload) {
            sum += (b & 0xFF);
        }
        
        return sum & 0xFFFFFFFF;
    }
    
    /**
     * Validate the message structure
     */
    public boolean validate() {
        // Check magic bytes
        if (magic == null || magic.length != MAGIC_BYTES.length) {
            return false;
        }
        for (int i = 0; i < MAGIC_BYTES.length; i++) {
            if (magic[i] != MAGIC_BYTES[i]) {
                return false;
            }
        }
        
        // Check protocol version
        if (protocolVersion != PROTOCOL_VERSION) {
            return false;
        }
        
        // Check message type
        if (messageType == null) {
            return false;
        }
        
        // Check payload length matches actual payload
        if (payloadLength != payload.length) {
            return false;
        }
        
        // Check payload length is reasonable
        if (payloadLength < 0 || payloadLength > 16 * 1024 * 1024) { // Max 16MB
            return false;
        }
        
        // Validate checksum
        int calculatedChecksum = calculateChecksum();
        if (checksum != 0 && checksum != calculatedChecksum) {
            return false;
        }
        
        // Validate fragment fields if fragmentation is enabled
        if (fragmentationEnabled) {
            if (fragmentIndex < 0 || fragmentIndex >= totalFragments) {
                return false;
            }
            if (totalFragments <= 0 || totalFragments > 1000) {
                return false;
            }
            if (fragmentOffset < 0 || fragmentOffset >= totalFragmentSize) {
                return false;
            }
            if (totalFragmentSize <= 0 || totalFragmentSize > 16 * 1024 * 1024) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Serialize message to byte array
     */
    public byte[] serialize() {
        // Calculate total size
        int headerSize = 4 + // magic
                        2 + // protocol version
                        1 + // message type
                        4 + // sequence number
                        4 + // session ID
                        4 + // flags
                        4 + // payload length
                        4 + // checksum
                        4 + // timestamp
                        4 + // fragment offset
                        4 + // total fragment size
                        2 + // fragment index
                        2;  // total fragments
        
        int extendedHeaderSize = 0;
        for (Map.Entry<String, String> entry : extendedHeaders.entrySet()) {
            extendedHeaderSize += 2 + entry.getKey().getBytes().length; // key length + key
            extendedHeaderSize += 2 + entry.getValue().getBytes().length; // value length + value
        }
        extendedHeaderSize += 2; // extended header count
        
        int totalSize = headerSize + extendedHeaderSize + payloadLength;
        if (totalSize < 0 || totalSize > 32 * 1024 * 1024) {
            throw new IllegalArgumentException("Message size exceeds maximum limit: " + totalSize);
        }
        
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        // Write magic bytes
        buffer.put(magic);
        
        // Write protocol version
        buffer.putShort((short) protocolVersion);
        
        // Write message type
        buffer.put(messageType.getCode());
        
        // Write sequence number
        buffer.putInt(sequenceNumber);
        
        // Write session ID
        buffer.putInt(sessionId);
        
        // Write flags
        buffer.putInt(flags);
        
        // Write payload length
        buffer.putInt(payloadLength);
        
        // Write checksum
        buffer.putInt(checksum);
        
        // Write timestamp
        buffer.putInt(timestamp);
        
        // Write fragment fields
        buffer.putInt(fragmentOffset);
        buffer.putInt(totalFragmentSize);
        buffer.putShort((short) fragmentIndex);
        buffer.putShort((short) totalFragments);
        
        // Write extended headers
        buffer.putShort((short) extendedHeaders.size());
        for (Map.Entry<String, String> entry : extendedHeaders.entrySet()) {
            byte[] keyBytes = entry.getKey().getBytes();
            byte[] valueBytes = entry.getValue().getBytes();
            buffer.putShort((short) keyBytes.length);
            buffer.put(keyBytes);
            buffer.putShort((short) valueBytes.length);
            buffer.put(valueBytes);
        }
        
        // Write payload
        buffer.put(payload);
        
        return buffer.array();
    }
    
    /**
     * Deserialize message from byte array
     */
    public static ProtocolMessage deserialize(byte[] data) {
        if (data == null || data.length < 35) { // Minimum header size
            return null;
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.BIG_ENDIAN);
        
        ProtocolMessage message = new ProtocolMessage();
        
        // Read magic bytes
        byte[] magic = new byte[4];
        buffer.get(magic);
        message.setMagic(magic);
        
        // Read protocol version
        message.setProtocolVersion(buffer.getShort() & 0xFFFF);
        
        // Read message type
        byte messageTypeCode = buffer.get();
        message.setMessageType(MessageType.fromCode(messageTypeCode));
        
        // Read sequence number
        message.setSequenceNumber(buffer.getInt());
        
        // Read session ID
        message.setSessionId(buffer.getInt());
        
        // Read flags
        message.setFlags(buffer.getInt());
        message.parseFlags();
        
        // Read payload length
        int payloadLength = buffer.getInt();
        if (payloadLength < 0 || payloadLength > 16 * 1024 * 1024 || payloadLength > buffer.remaining()) {
            return null;
        }
        message.setPayloadLength(payloadLength);
        
        // Read checksum
        message.setChecksum(buffer.getInt());
        
        // Read timestamp
        message.setTimestamp(buffer.getInt());
        
        // Read fragment fields
        message.setFragmentOffset(buffer.getInt());
        message.setTotalFragmentSize(buffer.getInt());
        message.setFragmentIndex(buffer.getShort() & 0xFFFF);
        message.setTotalFragments(buffer.getShort() & 0xFFFF);
        
        // Read extended headers
        int headerCount = buffer.getShort() & 0xFFFF;
        Map<String, String> headers = new HashMap<>();
        for (int i = 0; i < headerCount; i++) {
            int keyLength = buffer.getShort() & 0xFFFF;
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            String key = new String(keyBytes);
            
            int valueLength = buffer.getShort() & 0xFFFF;
            byte[] valueBytes = new byte[valueLength];
            buffer.get(valueBytes);
            String value = new String(valueBytes);
            
            headers.put(key, value);
        }
        message.setExtendedHeaders(headers);
        
        // Read payload
        if (message.payloadLength > 0 && buffer.remaining() >= message.payloadLength) {
            byte[] payload = new byte[message.payloadLength];
            buffer.get(payload);
            message.setPayload(payload);
        }
        
        return message;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProtocolMessage that = (ProtocolMessage) o;
        return protocolVersion == that.protocolVersion &&
               sequenceNumber == that.sequenceNumber &&
               sessionId == that.sessionId &&
               flags == that.flags &&
               payloadLength == that.payloadLength &&
               checksum == that.checksum &&
               timestamp == that.timestamp &&
               fragmentOffset == that.fragmentOffset &&
               totalFragmentSize == that.totalFragmentSize &&
               fragmentIndex == that.fragmentIndex &&
               totalFragments == that.totalFragments &&
               Arrays.equals(magic, that.magic) &&
               messageType == that.messageType &&
               Arrays.equals(payload, that.payload) &&
               Objects.equals(extendedHeaders, that.extendedHeaders);
    }
    
    @Override
    public int hashCode() {
        int result = Objects.hash(protocolVersion, messageType, sequenceNumber, sessionId, flags, 
                                 payloadLength, checksum, timestamp, extendedHeaders, 
                                 fragmentOffset, totalFragmentSize, fragmentIndex, totalFragments);
        result = 31 * result + Arrays.hashCode(magic);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
    
    @Override
    public String toString() {
        return "ProtocolMessage{" +
               "messageType=" + messageType +
               ", sequenceNumber=" + sequenceNumber +
               ", sessionId=" + sessionId +
               ", payloadLength=" + payloadLength +
               ", flags=" + flags +
               ", timestamp=" + timestamp +
               ", fragmentIndex=" + fragmentIndex +
               ", totalFragments=" + totalFragments +
               '}';
    }
}
