package com.aetherflow;

import com.aetherflow.ConnectionStateMachine;
import com.aetherflow.ConnectionStateMachine.StateEvent;
import com.aetherflow.ConnectionStateMachine.ConnectionState;
import java.util.concurrent.CountDownLatch;

public class StateMachineFuzzer {
    
    private static ConnectionStateMachine stateMachine;
    
    static {
        try {
            stateMachine = new ConnectionStateMachine(1);
        } catch (Exception e) {
            // Ignore
        }
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
            if (stateMachine == null) {
                return;
            }
            
            // Reset state machine
            stateMachine.reset();
            
            // Process bytes as event sequence
            for (int i = 0; i < data.length; i++) {
                byte b = data[i];
                StateEvent event = mapByteToEvent(b);
                
                if (event != null) {
                    stateMachine.transition(event);
                    ConnectionState currentState = stateMachine.getCurrentState();
                    stateMachine.getPossibleNextStates();
                    stateMachine.getStateVisitCount(currentState);
                    stateMachine.getTransitionHistory();
                    stateMachine.isTerminal();
                    stateMachine.isError();
                    stateMachine.isActive();
                }
            }
            
            if (data.length >= 2) {
                int stateId = data[0] & 0xFF;
                int eventId = data[1] & 0xFF;
                
                ConnectionState targetState = ConnectionState.fromId(stateId);
                StateEvent event = StateEvent.values()[eventId % StateEvent.values().length];
                
                if (targetState != null) {
                    stateMachine.forceTransition(targetState, event);
                }
            }
            
        } catch (RuntimeException e) {
            throw e; // Rethrow unexpected runtime exceptions to crash fuzzer
        } catch (Exception e) {
            // Ignore checked exceptions during fuzzing
        }
    }
    
    private static void runConcurrencyCheck() {
        try {
            final ConnectionStateMachine sm = new ConnectionStateMachine(2);
            final CountDownLatch startLatch = new CountDownLatch(1);
            final CountDownLatch finishLatch = new CountDownLatch(2);
            final Throwable[] exceptionHolder = new Throwable[1];

            Thread t1 = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 1000; i++) {
                        sm.addListener((connId, oldState, newState, event) -> {});
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
                        sm.transition(StateEvent.INITIALIZE);
                        sm.reset();
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

    private static StateEvent mapByteToEvent(byte b) {
        StateEvent[] events = StateEvent.values();
        return events[Math.abs(b) % events.length];
    }
}
