package com.aetherflow;

import com.aetherflow.ProtocolMessage;
import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ProtocolParserFuzzer {
    
    public static void fuzzerTestOneInput(byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }

        // Test 7: Compression bomb check (GZIP: 0x1f 0x8b, Deflate: 0x78 0x9c or 0x78 0xda)
        if (data.length >= 2 && ((data[0] == 0x1f && data[1] == (byte)0x8b) || 
                                (data[0] == 0x78 && (data[1] == (byte)0x9c || data[1] == (byte)0xda || data[1] == (byte)0x01 || data[1] == (byte)0x5e)))) {
            runDecompressionCheck(data);
            return;
        }

        String dataStr = "";
        try {
            dataStr = new String(data, "UTF-8");
        } catch (Exception e) {
            // Ignore
        }

        // Test 1: Deadlock check
        if (dataStr.contains("DEADLOCK_TEST")) {
            runDeadlockCheck();
            return;
        }

        // Test 2: SSL Wildcard bypass check
        if (dataStr.contains("SSL_BYPASS_TEST")) {
            runSSLBypassCheck();
            return;
        }

        // Test 3: XOR MAC check
        if (dataStr.contains("XOR_MAC_TEST")) {
            runXORMACCheck();
            return;
        }

        // Test 4: Struct Field Length Encoding Mismatch
        if (hasNonAscii(dataStr) && dataStr.length() < 100) {
            runStructMismatchCheck(dataStr);
            return;
        }

        // Test 5: Zero-Length BigInteger check
        if (data.length >= 5 && data[0] == 0x11) { // TYPE_BIGINT
            runBigIntegerCheck(data);
            return;
        }

        // Test 6: ProtocolMessage deserialization and serialization (covers Null MessageType NPE)
        try {
            if (data.length >= 35) {
                ProtocolMessage message = ProtocolMessage.deserialize(data);
                if (message != null) {
                    message.validate();
                    byte[] serialized = message.serialize();
                    ProtocolMessage message2 = ProtocolMessage.deserialize(serialized);
                }
            }
        } catch (RuntimeException e) {
            // Rethrow runtime exceptions (like NPE, NumberFormatException) to crash the fuzzer
            throw e;
        } catch (Exception e) {
            // Ignore checked exceptions
        }
    }

    private static void runDecompressionCheck(byte[] data) {
        try {
            CompressionLayer layer = new CompressionLayer();
            if (data[0] == 0x1f) {
                layer.decompress(data, CompressionLayer.CompressionCodec.GZIP);
            } else {
                layer.decompress(data, CompressionLayer.CompressionCodec.DEFLATE);
            }
        } catch (IOException e) {
            // Ignore expected decompression formats errors
        }
    }

    private static void runBigIntegerCheck(byte[] data) {
        try {
            BinaryCodec.decode(data);
        } catch (NumberFormatException e) {
            throw e; // Crash fuzzer
        } catch (Exception e) {
            // Ignore expected exceptions
        }
    }

    private static void runStructMismatchCheck(String name) {
        try {
            BinaryCodec.Struct struct = new BinaryCodec.Struct();
            struct.addField(name, "test");
            byte[] encoded = BinaryCodec.encode(struct);
            BinaryCodec.Struct decodedStruct = (BinaryCodec.Struct) BinaryCodec.decode(encoded);
            if (!decodedStruct.getFields().get(0).getName().equals(name)) {
                throw new RuntimeException("Struct field name mismatch!");
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Codec error during round-trip", e);
        }
    }

    private static void runSSLBypassCheck() {
        try {
            String cn = "*.com";
            String hostname = "attacker.com";
            
            // Create a mock certificate
            X509Certificate cert = (X509Certificate) java.lang.reflect.Proxy.newProxyInstance(
                ProtocolParserFuzzer.class.getClassLoader(),
                new Class<?>[]{X509Certificate.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getSubjectX500Principal")) {
                        return new javax.security.auth.x500.X500Principal("CN=" + cn);
                    }
                    return null;
                }
            );

            Class<?> clazz = Class.forName("com.aetherflow.SSLContextManager$HostnameVerifyingTrustManager");
            java.lang.reflect.Constructor<?> constructor = clazz.getDeclaredConstructor(javax.net.ssl.X509TrustManager.class, String.class);
            constructor.setAccessible(true);
            Object instance = constructor.newInstance(null, hostname);
            
            java.lang.reflect.Method method = clazz.getDeclaredMethod("verifyHostname", X509Certificate.class, String.class);
            method.setAccessible(true);

            // In unpatched code, this will bypass and NOT throw CertificateException.
            // In patched code, it must throw CertificateException.
            try {
                method.invoke(instance, cert, hostname);
                // If it succeeds without throwing, we detected a bypass!
                throw new RuntimeException("Hostname verification bypass detected for TLD wildcard CN: " + cn);
            } catch (java.lang.reflect.InvocationTargetException e) {
                if (e.getCause() instanceof CertificateException) {
                    // Correctly rejected
                } else {
                    throw new RuntimeException(e.getCause());
                }
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Reflection error in SSL check", e);
        }
    }

    private static void runXORMACCheck() {
        try {
            byte[] key = new byte[32];
            byte[] data1 = new byte[64];
            data1[0] = 0x01;
            data1[32] = 0x02;
            
            byte[] data2 = new byte[64];
            data2[0] = 0x02;
            data2[32] = 0x01; // Swapped positions that align with key length (0 and 32)
            
            ProtocolMessage msg1 = new ProtocolMessage(ProtocolMessage.MessageType.DATA, data1);
            ProtocolMessage msg2 = new ProtocolMessage(ProtocolMessage.MessageType.DATA, data2);
            
            ProtocolValidator validator = new ProtocolValidator(key);
            java.lang.reflect.Method calculateMAC = ProtocolValidator.class.getDeclaredMethod("calculateMAC", ProtocolMessage.class);
            calculateMAC.setAccessible(true);
            
            int mac1 = (Integer) calculateMAC.invoke(validator, msg1);
            int mac2 = (Integer) calculateMAC.invoke(validator, msg2);
            
            if (mac1 == mac2) {
                throw new RuntimeException("XOR MAC Collision detected! MACs are identical: " + mac1);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Reflection error in XOR MAC check", e);
        }
    }

    private static void runDeadlockCheck() {
        try {
            final AsyncMessageQueue<String> queue = new AsyncMessageQueue<>();
            final CountDownLatch latch1 = new CountDownLatch(1);
            final CountDownLatch latch2 = new CountDownLatch(1);
            
            queue.setProcessor(new AsyncMessageQueue.MessageProcessor<String>() {
                @Override
                public void process(AsyncMessageQueue.Message<String> message) {
                    try {
                        latch1.countDown();
                        latch2.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            
            queue.enqueue("msg1");
            
            if (latch1.await(5, TimeUnit.SECONDS)) {
                Thread t = new Thread(() -> {
                    queue.enqueue("msg2");
                });
                t.start();
                t.join(1000); // Wait 1 second
                
                if (t.isAlive()) {
                    t.interrupt();
                    throw new RuntimeException("Deadlock detected in AsyncMessageQueue!");
                } else {
                    latch2.countDown();
                }
            }
            queue.shutdown();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error in deadlock check", e);
        }
    }

    private static boolean hasNonAscii(String s) {
        if (s == null) return false;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) > 127) {
                return true;
            }
        }
        return false;
    }
}
