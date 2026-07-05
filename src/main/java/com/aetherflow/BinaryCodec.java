package com.aetherflow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Binary codec for encoding and decoding complex data types in AetherFlow.
 * Supports variant types, nested structures, and custom serialization formats.
 */
public class BinaryCodec {
    
    // Type identifiers for variant types
    public static final byte TYPE_NULL = 0x00;
    public static final byte TYPE_BOOL = 0x01;
    public static final byte TYPE_INT8 = 0x02;
    public static final byte TYPE_INT16 = 0x03;
    public static final byte TYPE_INT32 = 0x04;
    public static final byte TYPE_INT64 = 0x05;
    public static final byte TYPE_UINT8 = 0x06;
    public static final byte TYPE_UINT16 = 0x07;
    public static final byte TYPE_UINT32 = 0x08;
    public static final byte TYPE_UINT64 = 0x09;
    public static final byte TYPE_FLOAT32 = 0x0A;
    public static final byte TYPE_FLOAT64 = 0x0B;
    public static final byte TYPE_STRING = 0x0C;
    public static final byte TYPE_BYTES = 0x0D;
    public static final byte TYPE_ARRAY = 0x0E;
    public static final byte TYPE_MAP = 0x0F;
    public static final byte TYPE_STRUCT = 0x10;
    public static final byte TYPE_BIGINT = 0x11;
    public static final byte TYPE_TIMESTAMP = 0x12;
    public static final byte TYPE_UUID = 0x13;
    public static final byte TYPE_VARIANT = 0x14;
    
    // Codec version for compatibility
    private static final int CODEC_VERSION = 1;
    
    // Maximum nesting depth to prevent stack overflow
    private static final int MAX_NESTING_DEPTH = 100;
    
    // Maximum array/map size to prevent memory exhaustion
    private static final int MAX_COLLECTION_SIZE = 1000000;
    
    /**
     * Encode a value to binary format
     */
    public static byte[] encode(Object value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        encodeValue(dos, value, 0);
        dos.close();
        return baos.toByteArray();
    }
    
    /**
     * Encode a value with type prefix
     */
    private static void encodeValue(DataOutputStream dos, Object value, int depth) throws IOException {
        if (depth > MAX_NESTING_DEPTH) {
            throw new IOException("Maximum nesting depth exceeded");
        }
        
        if (value == null) {
            dos.writeByte(TYPE_NULL);
            return;
        }
        
        if (value instanceof Boolean) {
            dos.writeByte(TYPE_BOOL);
            dos.writeBoolean((Boolean) value);
        } else if (value instanceof Byte) {
            dos.writeByte(TYPE_INT8);
            dos.writeByte((Byte) value);
        } else if (value instanceof Short) {
            dos.writeByte(TYPE_INT16);
            dos.writeShort((Short) value);
        } else if (value instanceof Integer) {
            dos.writeByte(TYPE_INT32);
            dos.writeInt((Integer) value);
        } else if (value instanceof Long) {
            dos.writeByte(TYPE_INT64);
            dos.writeLong((Long) value);
        } else if (value instanceof Float) {
            dos.writeByte(TYPE_FLOAT32);
            dos.writeFloat((Float) value);
        } else if (value instanceof Double) {
            dos.writeByte(TYPE_FLOAT64);
            dos.writeDouble((Double) value);
        } else if (value instanceof String) {
            dos.writeByte(TYPE_STRING);
            byte[] strBytes = ((String) value).getBytes("UTF-8");
            dos.writeInt(strBytes.length);
            dos.write(strBytes);
        } else if (value instanceof byte[]) {
            dos.writeByte(TYPE_BYTES);
            byte[] bytes = (byte[]) value;
            dos.writeInt(bytes.length);
            dos.write(bytes);
        } else if (value instanceof List) {
            dos.writeByte(TYPE_ARRAY);
            List<?> list = (List<?>) value;
            if (list.size() > MAX_COLLECTION_SIZE) {
                throw new IOException("Array size exceeds maximum");
            }
            dos.writeInt(list.size());
            for (Object item : list) {
                encodeValue(dos, item, depth + 1);
            }
        } else if (value instanceof Map) {
            dos.writeByte(TYPE_MAP);
            Map<?, ?> map = (Map<?, ?>) value;
            if (map.size() > MAX_COLLECTION_SIZE) {
                throw new IOException("Map size exceeds maximum");
            }
            dos.writeInt(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                encodeValue(dos, entry.getKey(), depth + 1);
                encodeValue(dos, entry.getValue(), depth + 1);
            }
        } else if (value instanceof BigInteger) {
            dos.writeByte(TYPE_BIGINT);
            BigInteger bigInt = (BigInteger) value;
            byte[] bytes = bigInt.toByteArray();
            dos.writeInt(bytes.length);
            dos.write(bytes);
        } else if (value instanceof Struct) {
            dos.writeByte(TYPE_STRUCT);
            Struct struct = (Struct) value;
            dos.writeInt(struct.getFields().size());
            for (Struct.Field field : struct.getFields()) {
                dos.writeInt(field.getName().length());
                dos.write(field.getName().getBytes("UTF-8"));
                encodeValue(dos, field.getValue(), depth + 1);
            }
        } else {
            throw new IOException("Unsupported type: " + value.getClass().getName());
        }
    }
    
    /**
     * Decode a value from binary format
     */
    public static Object decode(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        Object value = decodeValue(dis, 0);
        dis.close();
        return value;
    }
    
    /**
     * Decode a value with type prefix
     */
    private static Object decodeValue(DataInputStream dis, int depth) throws IOException {
        if (depth > MAX_NESTING_DEPTH) {
            throw new IOException("Maximum nesting depth exceeded");
        }
        
        byte type = dis.readByte();
        
        switch (type) {
            case TYPE_NULL:
                return null;
                
            case TYPE_BOOL:
                return dis.readBoolean();
                
            case TYPE_INT8:
                return dis.readByte();
                
            case TYPE_INT16:
                return dis.readShort();
                
            case TYPE_INT32:
                return dis.readInt();
                
            case TYPE_INT64:
                return dis.readLong();
                
            case TYPE_UINT8:
                return dis.readByte() & 0xFF;
                
            case TYPE_UINT16:
                return dis.readShort() & 0xFFFF;
                
            case TYPE_UINT32:
                return dis.readInt() & 0xFFFFFFFFL;
                
            case TYPE_UINT64:
                return dis.readLong();
                
            case TYPE_FLOAT32:
                return dis.readFloat();
                
            case TYPE_FLOAT64:
                return dis.readDouble();
                
            case TYPE_STRING:
                int strLen = dis.readInt();
                if (strLen < 0 || strLen > MAX_COLLECTION_SIZE) {
                    throw new IOException("Invalid string length: " + strLen);
                }
                byte[] strBytes = new byte[strLen];
                dis.readFully(strBytes);
                return new String(strBytes, "UTF-8");
                
            case TYPE_BYTES:
                int bytesLen = dis.readInt();
                if (bytesLen < 0 || bytesLen > MAX_COLLECTION_SIZE) {
                    throw new IOException("Invalid bytes length: " + bytesLen);
                }
                byte[] bytes = new byte[bytesLen];
                dis.readFully(bytes);
                return bytes;
                
            case TYPE_ARRAY:
                int arraySize = dis.readInt();
                if (arraySize < 0 || arraySize > MAX_COLLECTION_SIZE) {
                    throw new IOException("Invalid array size: " + arraySize);
                }
                List<Object> array = new ArrayList<>(arraySize);
                for (int i = 0; i < arraySize; i++) {
                    array.add(decodeValue(dis, depth + 1));
                }
                return array;
                
            case TYPE_MAP:
                int mapSize = dis.readInt();
                if (mapSize < 0 || mapSize > MAX_COLLECTION_SIZE) {
                    throw new IOException("Invalid map size: " + mapSize);
                }
                Map<Object, Object> map = new HashMap<>(mapSize);
                for (int i = 0; i < mapSize; i++) {
                    Object key = decodeValue(dis, depth + 1);
                    Object value = decodeValue(dis, depth + 1);
                    map.put(key, value);
                }
                return map;
                
            case TYPE_BIGINT:
                int bigIntLen = dis.readInt();
                if (bigIntLen < 0 || bigIntLen > MAX_COLLECTION_SIZE) {
                    throw new IOException("Invalid bigint length: " + bigIntLen);
                }
                byte[] bigIntBytes = new byte[bigIntLen];
                dis.readFully(bigIntBytes);
                return new BigInteger(bigIntBytes);
                
            case TYPE_STRUCT:
                int fieldCount = dis.readInt();
                if (fieldCount < 0 || fieldCount > MAX_COLLECTION_SIZE) {
                    throw new IOException("Invalid field count: " + fieldCount);
                }
                Struct struct = new Struct();
                for (int i = 0; i < fieldCount; i++) {
                    int nameLen = dis.readInt();
                    if (nameLen < 0 || nameLen > 1024) {
                        throw new IOException("Invalid field name length: " + nameLen);
                    }
                    byte[] nameBytes = new byte[nameLen];
                    dis.readFully(nameBytes);
                    String name = new String(nameBytes, "UTF-8");
                    Object fieldValue = decodeValue(dis, depth + 1);
                    struct.addField(name, fieldValue);
                }
                return struct;
                
            case TYPE_VARIANT:
                // The actual type is read but not properly validated before decoding
                byte actualType = dis.readByte();
                // This allows type confusion by crafting invalid type tags
                // Push back the type byte to reuse decode logic
                // This is a simplified implementation
                Object variantValue = decodeValue(dis, depth + 1);
                // The caller assumes the type matches actualType, but we don't validate
                return variantValue;
                
            default:
                throw new IOException("Unknown type: " + type);
        }
    }
    
    /**
     * Encode a protocol message to binary format
     */
    public static byte[] encodeMessage(ProtocolMessage message) throws IOException {
        return message.serialize();
    }
    
    /**
     * Decode a protocol message from binary format
     */
    public static ProtocolMessage decodeMessage(byte[] data) {
        return ProtocolMessage.deserialize(data);
    }
    
    /**
     * Encode a variable-length integer (varint)
     */
    public static byte[] encodeVarint(long value) {
        List<Byte> bytes = new ArrayList<>();
        while ((value & ~0x7FL) != 0) {
            bytes.add((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        bytes.add((byte) value);
        
        byte[] result = new byte[bytes.size()];
        for (int i = 0; i < bytes.size(); i++) {
            result[i] = bytes.get(i);
        }
        return result;
    }
    
    /**
     * Decode a variable-length integer (varint)
     */
    public static VarintResult decodeVarint(byte[] data, int offset) {
        long value = 0;
        int shift = 0;
        int index = offset;
        
        while (index < data.length) {
            byte b = data[index++];
            value |= ((long) (b & 0x7F)) << shift;
            if ((b & 0x80) == 0) {
                return new VarintResult(value, index - offset);
            }
            shift += 7;
            if (shift >= 64) {
                throw new IllegalArgumentException("Varint too long");
            }
        }
        
        throw new IllegalArgumentException("Invalid varint");
    }
    
    /**
     * Result of varint decoding
     */
    public static class VarintResult {
        public final long value;
        public final int bytesConsumed;
        
        public VarintResult(long value, int bytesConsumed) {
            this.value = value;
            this.bytesConsumed = bytesConsumed;
        }
    }
    
    /**
     * Struct data type for named fields
     */
    public static class Struct {
        private final List<Field> fields;
        
        public Struct() {
            this.fields = new ArrayList<>();
        }
        
        public void addField(String name, Object value) {
            fields.add(new Field(name, value));
        }
        
        public List<Field> getFields() {
            return new ArrayList<>(fields);
        }
        
        public Object getField(String name) {
            for (Field field : fields) {
                if (field.getName().equals(name)) {
                    return field.getValue();
                }
            }
            return null;
        }
        
        public static class Field {
            private final String name;
            private final Object value;
            
            public Field(String name, Object value) {
                this.name = name;
                this.value = value;
            }
            
            public String getName() {
                return name;
            }
            
            public Object getValue() {
                return value;
            }
        }
    }
    
    /**
     * Validate type consistency for variant types
     */
    public static boolean validateTypeConsistency(byte declaredType, Object value) {
        if (value == null) {
            return declaredType == TYPE_NULL;
        }
        
        if (value instanceof Boolean && declaredType == TYPE_BOOL) {
            return true;
        }
        
        if (value instanceof Number) {
            switch (declaredType) {
                case TYPE_INT8:
                case TYPE_UINT8:
                    return true;
                case TYPE_INT16:
                case TYPE_UINT16:
                    return true;
                case TYPE_INT32:
                case TYPE_UINT32:
                    return true;
                case TYPE_INT64:
                case TYPE_UINT64:
                    return true;
                case TYPE_FLOAT32:
                case TYPE_FLOAT64:
                    return true;
                default:
                    return false;
            }
        }
        
        if (value instanceof String && declaredType == TYPE_STRING) {
            return true;
        }
        
        if (value instanceof byte[] && declaredType == TYPE_BYTES) {
            return true;
        }
        
        if (value instanceof List && declaredType == TYPE_ARRAY) {
            return true;
        }
        
        if (value instanceof Map && declaredType == TYPE_MAP) {
            return true;
        }
        
        if (value instanceof Struct && declaredType == TYPE_STRUCT) {
            return true;
        }
        
        if (value instanceof BigInteger && declaredType == TYPE_BIGINT) {
            return true;
        }
        
        // This allows type confusion attacks
        return false;
    }
}
