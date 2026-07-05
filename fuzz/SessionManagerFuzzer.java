package com.aetherflow;

import com.aetherflow.SessionManager;
import com.aetherflow.SessionManager.SessionEntry;
import java.util.concurrent.CountDownLatch;

public class SessionManagerFuzzer {
    
    private static SessionManager sessionManager;
    
    static {
        sessionManager = new SessionManager();
    }
    
    public static void fuzzerTestOneInput(byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }

        String dataStr = "";
        try {
            dataStr = new String(data, "UTF-8");
        } catch (Exception e) {
            // Ignore
        }

        if (dataStr.contains("CONCURRENCY_TEST")) {
            runConcurrencyCheck();
            return;
        }

        try {
            int offset = 0;
            while (offset < data.length - 4) {
                byte op = data[offset];
                offset++;
                
                switch (op) {
                    case 0: // Create session
                        if (offset + 8 <= data.length) {
                            String userId = "user_" + ((data[offset] & 0xFF));
                            String authToken = "token_" + ((data[offset + 1] & 0xFF));
                            offset += 8;
                            
                            SessionEntry session = sessionManager.createSession(userId, authToken);
                            if (session != null) {
                                session.getSessionId();
                                session.getUserId();
                                session.isExpired();
                                session.getAge();
                                session.getIdleTime();
                            }
                        }
                        break;
                        
                    case 1: // Validate session
                        if (offset + 16 <= data.length) {
                            String sessionId = "sess_" + ((data[offset] & 0xFF));
                            offset += 16;
                            
                            boolean valid = sessionManager.validateSession(sessionId);
                            SessionEntry session = sessionManager.getSession(sessionId);
                            if (session != null) {
                                session.recordAccess();
                            }
                        }
                        break;
                        
                    case 2: // Authenticate session
                        if (offset + 16 <= data.length) {
                            String sessionId = "sess_" + ((data[offset] & 0xFF));
                            String role = "role_" + ((data[offset + 1] & 0xFF));
                            offset += 16;
                            
                            sessionManager.authenticateSession(sessionId, role);
                        }
                        break;
                        
                    case 3: // Grant permission
                        if (offset + 16 <= data.length) {
                            String sessionId = "sess_" + ((data[offset] & 0xFF));
                            String permission = "perm_" + ((data[offset + 1] & 0xFF));
                            offset += 16;
                            
                            sessionManager.grantPermission(sessionId, permission);
                        }
                        break;
                        
                    case 4: // Refresh session
                        if (offset + 16 <= data.length) {
                            String sessionId = "sess_" + ((data[offset] & 0xFF));
                            offset += 16;
                            
                            sessionManager.refreshSession(sessionId);
                        }
                        break;
                        
                    case 5: // Revoke session
                        if (offset + 16 <= data.length) {
                            String sessionId = "sess_" + ((data[offset] & 0xFF));
                            offset += 16;
                            
                            sessionManager.revokeSession(sessionId);
                        }
                        break;
                        
                    default:
                        offset++;
                        break;
                }
            }
            sessionManager.getStats();
        } catch (RuntimeException e) {
            throw e; // Rethrow unexpected runtime exceptions to crash fuzzer
        } catch (Exception e) {
            // Ignore checked exceptions during fuzzing
        }
    }

    private static void runConcurrencyCheck() {
        try {
            final SessionManager sm = new SessionManager();
            final SessionEntry session = sm.createSession("user_concurrency", "token_concurrency");
            final CountDownLatch startLatch = new CountDownLatch(1);
            final CountDownLatch finishLatch = new CountDownLatch(2);
            final Throwable[] exceptionHolder = new Throwable[1];

            Thread t1 = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 1000; i++) {
                        session.addPermission("perm_" + i);
                    }
                } catch (Exception e) {
                    // Ignore
                } finally {
                    finishLatch.countDown();
                }
            });

            Thread t2 = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 1000; i++) {
                        session.hasPermission("perm_nonexistent");
                    }
                } catch (Throwable e) {
                    exceptionHolder[0] = e;
                } finally {
                    finishLatch.countDown();
                }
            });

            t1.start();
            t2.start();
            startLatch.countDown();
            finishLatch.await();

            if (exceptionHolder[0] != null) {
                if (exceptionHolder[0] instanceof java.util.ConcurrentModificationException) {
                    throw (java.util.ConcurrentModificationException) exceptionHolder[0];
                } else if (exceptionHolder[0] instanceof RuntimeException) {
                    throw (RuntimeException) exceptionHolder[0];
                } else {
                    throw new RuntimeException(exceptionHolder[0]);
                }
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error in concurrency check", e);
        }
    }
}
