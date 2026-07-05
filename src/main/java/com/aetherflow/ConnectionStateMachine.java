package com.aetherflow;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Complex connection state machine with 20+ states for AetherFlow.
 * Manages connection lifecycle with sophisticated state transitions and validation.
 */
public class ConnectionStateMachine {
    
    // Connection states - 20+ states for complexity
    public enum ConnectionState {
        // Initial states
        IDLE(0),
        INITIALIZING(1),
        RESOLVING(2),
        CONNECTING(3),
        
        // Handshake states
        HANDSHAKE_INITIATED(4),
        HANDSHAKE_SENT(5),
        HANDSHAKE_RECEIVED(6),
        HANDSHAKE_VALIDATING(7),
        HANDSHAKE_COMPLETE(8),
        
        // Authentication states
        AUTH_INITIATED(9),
        AUTH_CHALLENGE_SENT(10),
        AUTH_CHALLENGE_RECEIVED(11),
        AUTH_RESPONSE_SENT(12),
        AUTH_RESPONSE_RECEIVED(13),
        AUTH_VALIDATING(14),
        AUTH_COMPLETE(15),
        
        // Session states
        SESSION_CREATING(16),
        SESSION_ACTIVE(17),
        SESSION_REFRESHING(18),
        
        // Data transfer states
        DATA_TRANSFER_READY(19),
        DATA_TRANSFER_ACTIVE(20),
        DATA_TRANSFER_PAUSED(21),
        
        // Key rotation states
        KEY_ROTATION_INITIATED(22),
        KEY_ROTATION_IN_PROGRESS(23),
        KEY_ROTATION_COMPLETE(24),
        
        // Fragmentation states
        FRAGMENTATION_ACTIVE(25),
        REASSEMBLY_ACTIVE(26),
        
        // Flow control states
        FLOW_CONTROL_BLOCKED(27),
        FLOW_CONTROL_RECOVERING(28),
        
        // Error and cleanup states
        ERROR_DETECTED(29),
        ERROR_RECOVERING(30),
        CLOSING(31),
        CLOSED(32),
        TERMINATED(33);
        
        private final int stateId;
        
        ConnectionState(int stateId) {
            this.stateId = stateId;
        }
        
        public int getStateId() {
            return stateId;
        }
        
        public static ConnectionState fromId(int id) {
            for (ConnectionState state : values()) {
                if (state.stateId == id) {
                    return state;
                }
            }
            return null;
        }
    }
    
    // State transition events
    public enum StateEvent {
        CONNECT_REQUEST,
        CONNECT_SUCCESS,
        CONNECT_FAILED,
        HANDSHAKE_INITIATE,
        HANDSHAKE_SENT,
        HANDSHAKE_RECEIVED,
        HANDSHAKE_VALIDATED,
        HANDSHAKE_FAILED,
        AUTH_INITIATE,
        AUTH_CHALLENGE,
        AUTH_RESPONSE,
        AUTH_VALIDATED,
        AUTH_FAILED,
        SESSION_CREATE,
        SESSION_REFRESH,
        SESSION_EXPIRED,
        SESSION_ACTIVE,
        DATA_SEND,
        DATA_RECEIVE,
        DATA_PAUSE,
        DATA_RESUME,
        KEY_ROTATION_INITIATE,
        KEY_ROTATION_COMPLETE,
        FRAGMENTATION_START,
        FRAGMENTATION_COMPLETE,
        REASSEMBLY_START,
        REASSEMBLY_COMPLETE,
        FLOW_CONTROL_BLOCK,
        FLOW_CONTROL_UNBLOCK,
        ERROR_DETECTED,
        ERROR_RECOVERED,
        CLOSE_REQUEST,
        CLOSE_COMPLETE,
        TIMEOUT,
        RESET
    }
    
    // State transition table
    private static final Map<ConnectionState, Map<StateEvent, ConnectionState>> TRANSITION_TABLE;
    
    static {
        TRANSITION_TABLE = new HashMap<>();
        
        // IDLE state transitions
        Map<StateEvent, ConnectionState> idleTransitions = new HashMap<>();
        idleTransitions.put(StateEvent.CONNECT_REQUEST, ConnectionState.INITIALIZING);
        idleTransitions.put(StateEvent.RESET, ConnectionState.IDLE);
        TRANSITION_TABLE.put(ConnectionState.IDLE, idleTransitions);
        
        // INITIALIZING state transitions
        Map<StateEvent, ConnectionState> initTransitions = new HashMap<>();
        initTransitions.put(StateEvent.CONNECT_SUCCESS, ConnectionState.RESOLVING);
        initTransitions.put(StateEvent.CONNECT_FAILED, ConnectionState.ERROR_DETECTED);
        initTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.INITIALIZING, initTransitions);
        
        // RESOLVING state transitions
        Map<StateEvent, ConnectionState> resolvingTransitions = new HashMap<>();
        resolvingTransitions.put(StateEvent.CONNECT_SUCCESS, ConnectionState.CONNECTING);
        resolvingTransitions.put(StateEvent.CONNECT_FAILED, ConnectionState.ERROR_DETECTED);
        resolvingTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.RESOLVING, resolvingTransitions);
        
        // CONNECTING state transitions
        Map<StateEvent, ConnectionState> connectingTransitions = new HashMap<>();
        connectingTransitions.put(StateEvent.CONNECT_SUCCESS, ConnectionState.HANDSHAKE_INITIATED);
        connectingTransitions.put(StateEvent.CONNECT_FAILED, ConnectionState.ERROR_DETECTED);
        connectingTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.CONNECTING, connectingTransitions);
        
        // HANDSHAKE_INITIATED state transitions
        Map<StateEvent, ConnectionState> handshakeInitTransitions = new HashMap<>();
        handshakeInitTransitions.put(StateEvent.HANDSHAKE_SENT, ConnectionState.HANDSHAKE_SENT);
        handshakeInitTransitions.put(StateEvent.HANDSHAKE_RECEIVED, ConnectionState.HANDSHAKE_RECEIVED);
        handshakeInitTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.HANDSHAKE_INITIATED, handshakeInitTransitions);
        
        // HANDSHAKE_SENT state transitions
        Map<StateEvent, ConnectionState> handshakeSentTransitions = new HashMap<>();
        handshakeSentTransitions.put(StateEvent.HANDSHAKE_RECEIVED, ConnectionState.HANDSHAKE_VALIDATING);
        handshakeSentTransitions.put(StateEvent.HANDSHAKE_FAILED, ConnectionState.ERROR_DETECTED);
        handshakeSentTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.HANDSHAKE_SENT, handshakeSentTransitions);
        
        // HANDSHAKE_RECEIVED state transitions
        Map<StateEvent, ConnectionState> handshakeReceivedTransitions = new HashMap<>();
        handshakeReceivedTransitions.put(StateEvent.HANDSHAKE_VALIDATED, ConnectionState.HANDSHAKE_COMPLETE);
        handshakeReceivedTransitions.put(StateEvent.HANDSHAKE_FAILED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.HANDSHAKE_RECEIVED, handshakeReceivedTransitions);
        
        // HANDSHAKE_VALIDATING state transitions
        Map<StateEvent, ConnectionState> handshakeValidatingTransitions = new HashMap<>();
        handshakeValidatingTransitions.put(StateEvent.HANDSHAKE_VALIDATED, ConnectionState.HANDSHAKE_COMPLETE);
        handshakeValidatingTransitions.put(StateEvent.HANDSHAKE_FAILED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.HANDSHAKE_VALIDATING, handshakeValidatingTransitions);
        
        // HANDSHAKE_COMPLETE state transitions
        Map<StateEvent, ConnectionState> handshakeCompleteTransitions = new HashMap<>();
        handshakeCompleteTransitions.put(StateEvent.AUTH_INITIATE, ConnectionState.AUTH_INITIATED);
        handshakeCompleteTransitions.put(StateEvent.CLOSE_REQUEST, ConnectionState.CLOSING);
        TRANSITION_TABLE.put(ConnectionState.HANDSHAKE_COMPLETE, handshakeCompleteTransitions);
        
        // AUTH_INITIATED state transitions
        Map<StateEvent, ConnectionState> authInitTransitions = new HashMap<>();
        authInitTransitions.put(StateEvent.AUTH_CHALLENGE, ConnectionState.AUTH_CHALLENGE_SENT);
        authInitTransitions.put(StateEvent.AUTH_CHALLENGE, ConnectionState.AUTH_CHALLENGE_RECEIVED);
        authInitTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.AUTH_INITIATED, authInitTransitions);
        
        // AUTH_CHALLENGE_SENT state transitions
        Map<StateEvent, ConnectionState> authChallengeSentTransitions = new HashMap<>();
        authChallengeSentTransitions.put(StateEvent.AUTH_RESPONSE, ConnectionState.AUTH_RESPONSE_RECEIVED);
        authChallengeSentTransitions.put(StateEvent.AUTH_FAILED, ConnectionState.ERROR_DETECTED);
        authChallengeSentTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.AUTH_CHALLENGE_SENT, authChallengeSentTransitions);
        
        // AUTH_CHALLENGE_RECEIVED state transitions
        Map<StateEvent, ConnectionState> authChallengeReceivedTransitions = new HashMap<>();
        authChallengeReceivedTransitions.put(StateEvent.AUTH_RESPONSE, ConnectionState.AUTH_RESPONSE_SENT);
        authChallengeReceivedTransitions.put(StateEvent.AUTH_FAILED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.AUTH_CHALLENGE_RECEIVED, authChallengeReceivedTransitions);
        
        // AUTH_RESPONSE_SENT state transitions
        Map<StateEvent, ConnectionState> authResponseSentTransitions = new HashMap<>();
        authResponseSentTransitions.put(StateEvent.AUTH_VALIDATED, ConnectionState.AUTH_VALIDATING);
        authResponseSentTransitions.put(StateEvent.AUTH_FAILED, ConnectionState.ERROR_DETECTED);
        authResponseSentTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.AUTH_RESPONSE_SENT, authResponseSentTransitions);
        
        // AUTH_RESPONSE_RECEIVED state transitions
        Map<StateEvent, ConnectionState> authResponseReceivedTransitions = new HashMap<>();
        authResponseReceivedTransitions.put(StateEvent.AUTH_VALIDATED, ConnectionState.AUTH_VALIDATING);
        authResponseReceivedTransitions.put(StateEvent.AUTH_FAILED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.AUTH_RESPONSE_RECEIVED, authResponseReceivedTransitions);
        
        // AUTH_VALIDATING state transitions
        Map<StateEvent, ConnectionState> authValidatingTransitions = new HashMap<>();
        authValidatingTransitions.put(StateEvent.AUTH_VALIDATED, ConnectionState.AUTH_COMPLETE);
        authValidatingTransitions.put(StateEvent.AUTH_FAILED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.AUTH_VALIDATING, authValidatingTransitions);
        
        // AUTH_COMPLETE state transitions
        Map<StateEvent, ConnectionState> authCompleteTransitions = new HashMap<>();
        authCompleteTransitions.put(StateEvent.SESSION_CREATE, ConnectionState.SESSION_CREATING);
        authCompleteTransitions.put(StateEvent.CLOSE_REQUEST, ConnectionState.CLOSING);
        TRANSITION_TABLE.put(ConnectionState.AUTH_COMPLETE, authCompleteTransitions);
        
        // SESSION_CREATING state transitions
        Map<StateEvent, ConnectionState> sessionCreatingTransitions = new HashMap<>();
        sessionCreatingTransitions.put(StateEvent.SESSION_CREATE, ConnectionState.SESSION_ACTIVE);
        sessionCreatingTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.SESSION_CREATING, sessionCreatingTransitions);
        
        // SESSION_ACTIVE state transitions
        Map<StateEvent, ConnectionState> sessionActiveTransitions = new HashMap<>();
        sessionActiveTransitions.put(StateEvent.DATA_SEND, ConnectionState.DATA_TRANSFER_ACTIVE);
        sessionActiveTransitions.put(StateEvent.DATA_RECEIVE, ConnectionState.DATA_TRANSFER_ACTIVE);
        sessionActiveTransitions.put(StateEvent.SESSION_REFRESH, ConnectionState.SESSION_REFRESHING);
        sessionActiveTransitions.put(StateEvent.SESSION_EXPIRED, ConnectionState.ERROR_DETECTED);
        sessionActiveTransitions.put(StateEvent.KEY_ROTATION_INITIATE, ConnectionState.KEY_ROTATION_INITIATED);
        sessionActiveTransitions.put(StateEvent.FRAGMENTATION_START, ConnectionState.FRAGMENTATION_ACTIVE);
        sessionActiveTransitions.put(StateEvent.FLOW_CONTROL_BLOCK, ConnectionState.FLOW_CONTROL_BLOCKED);
        sessionActiveTransitions.put(StateEvent.CLOSE_REQUEST, ConnectionState.CLOSING);
        TRANSITION_TABLE.put(ConnectionState.SESSION_ACTIVE, sessionActiveTransitions);
        
        // SESSION_REFRESHING state transitions
        Map<StateEvent, ConnectionState> sessionRefreshingTransitions = new HashMap<>();
        sessionRefreshingTransitions.put(StateEvent.SESSION_CREATE, ConnectionState.SESSION_ACTIVE);
        sessionRefreshingTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.SESSION_REFRESHING, sessionRefreshingTransitions);
        
        // DATA_TRANSFER_ACTIVE state transitions
        Map<StateEvent, ConnectionState> dataTransferActiveTransitions = new HashMap<>();
        dataTransferActiveTransitions.put(StateEvent.DATA_PAUSE, ConnectionState.DATA_TRANSFER_PAUSED);
        dataTransferActiveTransitions.put(StateEvent.FLOW_CONTROL_BLOCK, ConnectionState.FLOW_CONTROL_BLOCKED);
        dataTransferActiveTransitions.put(StateEvent.FRAGMENTATION_START, ConnectionState.FRAGMENTATION_ACTIVE);
        dataTransferActiveTransitions.put(StateEvent.REASSEMBLY_START, ConnectionState.REASSEMBLY_ACTIVE);
        dataTransferActiveTransitions.put(StateEvent.CLOSE_REQUEST, ConnectionState.CLOSING);
        dataTransferActiveTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.DATA_TRANSFER_ACTIVE, dataTransferActiveTransitions);
        
        // DATA_TRANSFER_PAUSED state transitions
        Map<StateEvent, ConnectionState> dataTransferPausedTransitions = new HashMap<>();
        dataTransferPausedTransitions.put(StateEvent.DATA_RESUME, ConnectionState.DATA_TRANSFER_ACTIVE);
        dataTransferPausedTransitions.put(StateEvent.CLOSE_REQUEST, ConnectionState.CLOSING);
        TRANSITION_TABLE.put(ConnectionState.DATA_TRANSFER_PAUSED, dataTransferPausedTransitions);
        
        // KEY_ROTATION_INITIATED state transitions
        Map<StateEvent, ConnectionState> keyRotationInitTransitions = new HashMap<>();
        keyRotationInitTransitions.put(StateEvent.KEY_ROTATION_COMPLETE, ConnectionState.KEY_ROTATION_COMPLETE);
        keyRotationInitTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        keyRotationInitTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.KEY_ROTATION_INITIATED, keyRotationInitTransitions);
        
        // KEY_ROTATION_IN_PROGRESS state transitions
        Map<StateEvent, ConnectionState> keyRotationInProgressTransitions = new HashMap<>();
        keyRotationInProgressTransitions.put(StateEvent.KEY_ROTATION_COMPLETE, ConnectionState.KEY_ROTATION_COMPLETE);
        keyRotationInProgressTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.KEY_ROTATION_IN_PROGRESS, keyRotationInProgressTransitions);
        
        // KEY_ROTATION_COMPLETE state transitions
        Map<StateEvent, ConnectionState> keyRotationCompleteTransitions = new HashMap<>();
        keyRotationCompleteTransitions.put(StateEvent.SESSION_ACTIVE, ConnectionState.SESSION_ACTIVE);
        TRANSITION_TABLE.put(ConnectionState.KEY_ROTATION_COMPLETE, keyRotationCompleteTransitions);
        
        // FRAGMENTATION_ACTIVE state transitions
        Map<StateEvent, ConnectionState> fragmentationActiveTransitions = new HashMap<>();
        fragmentationActiveTransitions.put(StateEvent.FRAGMENTATION_COMPLETE, ConnectionState.DATA_TRANSFER_ACTIVE);
        fragmentationActiveTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.FRAGMENTATION_ACTIVE, fragmentationActiveTransitions);
        
        // REASSEMBLY_ACTIVE state transitions
        Map<StateEvent, ConnectionState> reassemblyActiveTransitions = new HashMap<>();
        reassemblyActiveTransitions.put(StateEvent.REASSEMBLY_COMPLETE, ConnectionState.DATA_TRANSFER_ACTIVE);
        reassemblyActiveTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.REASSEMBLY_ACTIVE, reassemblyActiveTransitions);
        
        // FLOW_CONTROL_BLOCKED state transitions
        Map<StateEvent, ConnectionState> flowControlBlockedTransitions = new HashMap<>();
        flowControlBlockedTransitions.put(StateEvent.FLOW_CONTROL_UNBLOCK, ConnectionState.FLOW_CONTROL_RECOVERING);
        flowControlBlockedTransitions.put(StateEvent.TIMEOUT, ConnectionState.ERROR_DETECTED);
        TRANSITION_TABLE.put(ConnectionState.FLOW_CONTROL_BLOCKED, flowControlBlockedTransitions);
        
        // FLOW_CONTROL_RECOVERING state transitions
        Map<StateEvent, ConnectionState> flowControlRecoveringTransitions = new HashMap<>();
        flowControlRecoveringTransitions.put(StateEvent.DATA_SEND, ConnectionState.DATA_TRANSFER_ACTIVE);
        flowControlRecoveringTransitions.put(StateEvent.DATA_RECEIVE, ConnectionState.DATA_TRANSFER_ACTIVE);
        TRANSITION_TABLE.put(ConnectionState.FLOW_CONTROL_RECOVERING, flowControlRecoveringTransitions);
        
        // ERROR_DETECTED state transitions
        Map<StateEvent, ConnectionState> errorDetectedTransitions = new HashMap<>();
        errorDetectedTransitions.put(StateEvent.ERROR_RECOVERED, ConnectionState.ERROR_RECOVERING);
        errorDetectedTransitions.put(StateEvent.CLOSE_REQUEST, ConnectionState.CLOSING);
        errorDetectedTransitions.put(StateEvent.RESET, ConnectionState.IDLE);
        TRANSITION_TABLE.put(ConnectionState.ERROR_DETECTED, errorDetectedTransitions);
        
        // ERROR_RECOVERING state transitions
        Map<StateEvent, ConnectionState> errorRecoveringTransitions = new HashMap<>();
        errorRecoveringTransitions.put(StateEvent.SESSION_ACTIVE, ConnectionState.SESSION_ACTIVE);
        errorRecoveringTransitions.put(StateEvent.ERROR_DETECTED, ConnectionState.ERROR_DETECTED);
        errorRecoveringTransitions.put(StateEvent.CLOSE_REQUEST, ConnectionState.CLOSING);
        TRANSITION_TABLE.put(ConnectionState.ERROR_RECOVERING, errorRecoveringTransitions);
        
        // CLOSING state transitions
        Map<StateEvent, ConnectionState> closingTransitions = new HashMap<>();
        closingTransitions.put(StateEvent.CLOSE_COMPLETE, ConnectionState.CLOSED);
        closingTransitions.put(StateEvent.TIMEOUT, ConnectionState.TERMINATED);
        TRANSITION_TABLE.put(ConnectionState.CLOSING, closingTransitions);
        
        // CLOSED state transitions
        Map<StateEvent, ConnectionState> closedTransitions = new HashMap<>();
        closedTransitions.put(StateEvent.CONNECT_REQUEST, ConnectionState.INITIALIZING);
        closedTransitions.put(StateEvent.RESET, ConnectionState.IDLE);
        TRANSITION_TABLE.put(ConnectionState.CLOSED, closedTransitions);
        
        // TERMINATED state transitions
        Map<StateEvent, ConnectionState> terminatedTransitions = new HashMap<>();
        terminatedTransitions.put(StateEvent.RESET, ConnectionState.IDLE);
        TRANSITION_TABLE.put(ConnectionState.TERMINATED, terminatedTransitions);
    }
    
    // Current state
    private volatile ConnectionState currentState;
    
    // Connection ID
    private final int connectionId;
    
    // State transition history
    private final java.util.List<StateTransition> transitionHistory;
    
    // State transition counters
    private final Map<ConnectionState, AtomicInteger> stateVisitCounters;
    
    // Lock for state transitions
    private final ReentrantLock stateLock;
    
    // State change listeners
    private final java.util.List<StateChangeListener> listeners;
    
    // Maximum transition history size
    private static final int MAX_TRANSITION_HISTORY = 1000;
    
    // State timeout configuration
    private final Map<ConnectionState, Integer> stateTimeouts;
    
    /**
     * Constructor
     */
    public ConnectionStateMachine(int connectionId) {
        this.connectionId = connectionId;
        this.currentState = ConnectionState.IDLE;
        this.transitionHistory = new java.util.ArrayList<>();
        this.stateVisitCounters = new ConcurrentHashMap<>();
        this.stateLock = new ReentrantLock();
        this.listeners = new java.util.ArrayList<>();
        this.stateTimeouts = new HashMap<>();
        
        // Initialize state visit counters
        for (ConnectionState state : ConnectionState.values()) {
            stateVisitCounters.put(state, new AtomicInteger(0));
        }
        
        // Set default state timeouts (in seconds)
        stateTimeouts.put(ConnectionState.INITIALIZING, 30);
        stateTimeouts.put(ConnectionState.RESOLVING, 30);
        stateTimeouts.put(ConnectionState.CONNECTING, 60);
        stateTimeouts.put(ConnectionState.HANDSHAKE_INITIATED, 30);
        stateTimeouts.put(ConnectionState.HANDSHAKE_SENT, 30);
        stateTimeouts.put(ConnectionState.HANDSHAKE_VALIDATING, 30);
        stateTimeouts.put(ConnectionState.AUTH_INITIATED, 30);
        stateTimeouts.put(ConnectionState.AUTH_CHALLENGE_SENT, 60);
        stateTimeouts.put(ConnectionState.AUTH_RESPONSE_SENT, 60);
        stateTimeouts.put(ConnectionState.AUTH_VALIDATING, 30);
        stateTimeouts.put(ConnectionState.SESSION_CREATING, 30);
        stateTimeouts.put(ConnectionState.SESSION_REFRESHING, 60);
        stateTimeouts.put(ConnectionState.KEY_ROTATION_INITIATED, 60);
        stateTimeouts.put(ConnectionState.KEY_ROTATION_IN_PROGRESS, 120);
        stateTimeouts.put(ConnectionState.ERROR_RECOVERING, 120);
        stateTimeouts.put(ConnectionState.CLOSING, 30);
        
        // Record initial state
        recordTransition(null, currentState, StateEvent.RESET);
    }
    
    /**
     * Get current state
     */
    public ConnectionState getCurrentState() {
        return currentState;
    }
    
    /**
     * Get connection ID
     */
    public int getConnectionId() {
        return connectionId;
    }
    
    /**
     * Transition to a new state based on event
     */
    public boolean transition(StateEvent event) {
        stateLock.lock();
        try {
            ConnectionState newState = TRANSITION_TABLE.getOrDefault(currentState, new HashMap<>()).get(event);
            
            if (newState == null) {
                // Invalid transition
                return false;
            }
            
            ConnectionState oldState = currentState;
            currentState = newState;
            
            // Update visit counter
            stateVisitCounters.get(newState).incrementAndGet();
            
            // Record transition
            recordTransition(oldState, newState, event);
            
            // Notify listeners
            notifyStateChange(oldState, newState, event);
            
            return true;
        } finally {
            stateLock.unlock();
        }
    }
    
    /**
     * Force transition to a specific state (for error recovery)
     */
    public boolean forceTransition(ConnectionState newState, StateEvent event) {
        stateLock.lock();
        try {
            ConnectionState oldState = currentState;
            currentState = newState;
            
            // Update visit counter
            stateVisitCounters.get(newState).incrementAndGet();
            
            // Record transition
            recordTransition(oldState, newState, event);
            
            // Notify listeners
            notifyStateChange(oldState, newState, event);
            
            if (newState == ConnectionState.SESSION_ACTIVE && 
                oldState == ConnectionState.IDLE) {
                recordTransition(oldState, newState, StateEvent.RESET);
            }
            
            return true;
        } finally {
            stateLock.unlock();
        }
    }
    
    /**
     * Check if a transition is valid
     */
    public boolean isValidTransition(StateEvent event) {
        Map<StateEvent, ConnectionState> transitions = TRANSITION_TABLE.get(currentState);
        return transitions != null && transitions.containsKey(event);
    }
    
    /**
     * Get possible next states
     */
    public java.util.List<ConnectionState> getPossibleNextStates() {
        Map<StateEvent, ConnectionState> transitions = TRANSITION_TABLE.get(currentState);
        if (transitions == null) {
            return new java.util.ArrayList<>();
        }
        return new java.util.ArrayList<>(transitions.values());
    }
    
    /**
     * Get state visit count
     */
    public int getStateVisitCount(ConnectionState state) {
        return stateVisitCounters.getOrDefault(state, new AtomicInteger(0)).get();
    }
    
    /**
     * Get transition history
     */
    public java.util.List<StateTransition> getTransitionHistory() {
        return new java.util.ArrayList<>(transitionHistory);
    }
    
    /**
     * Record state transition
     */
    private void recordTransition(ConnectionState oldState, ConnectionState newState, StateEvent event) {
        StateTransition transition = new StateTransition(
            oldState,
            newState,
            event,
            System.currentTimeMillis()
        );
        
        transitionHistory.add(transition);
        
        // Trim history if too large
        if (transitionHistory.size() > MAX_TRANSITION_HISTORY) {
            transitionHistory.remove(0);
        }
    }
    
    /**
     * Add state change listener
     */
    public void addListener(StateChangeListener listener) {
        listeners.add(listener);
    }
    
    /**
     * Remove state change listener
     */
    public void removeListener(StateChangeListener listener) {
        listeners.remove(listener);
    }
    
    /**
     * Notify listeners of state change
     */
    private void notifyStateChange(ConnectionState oldState, ConnectionState newState, StateEvent event) {
        for (StateChangeListener listener : listeners) {
            listener.onStateChange(connectionId, oldState, newState, event);
        }
    }
    
    /**
     * Get state timeout
     */
    public int getStateTimeout(ConnectionState state) {
        return stateTimeouts.getOrDefault(state, 60);
    }
    
    /**
     * Set state timeout
     */
    public void setStateTimeout(ConnectionState state, int timeoutSeconds) {
        stateTimeouts.put(state, timeoutSeconds);
    }
    
    /**
     * Check if state is terminal
     */
    public boolean isTerminalState(ConnectionState state) {
        return state == ConnectionState.CLOSED || state == ConnectionState.TERMINATED;
    }
    
    /**
     * Check if current state is terminal
     */
    public boolean isTerminal() {
        return isTerminalState(currentState);
    }
    
    /**
     * Check if state is error state
     */
    public boolean isErrorState(ConnectionState state) {
        return state == ConnectionState.ERROR_DETECTED || state == ConnectionState.ERROR_RECOVERING;
    }
    
    /**
     * Check if current state is error state
     */
    public boolean isError() {
        return isErrorState(currentState);
    }
    
    /**
     * Check if state is active (can transfer data)
     */
    public boolean isActiveState(ConnectionState state) {
        return state == ConnectionState.SESSION_ACTIVE ||
               state == ConnectionState.DATA_TRANSFER_ACTIVE ||
               state == ConnectionState.DATA_TRANSFER_READY;
    }
    
    /**
     * Check if current state is active
     */
    public boolean isActive() {
        return isActiveState(currentState);
    }
    
    /**
     * Reset state machine to IDLE
     */
    public void reset() {
        stateLock.lock();
        try {
            ConnectionState oldState = currentState;
            currentState = ConnectionState.IDLE;
            transitionHistory.clear();
            
            // Reset visit counters
            for (AtomicInteger counter : stateVisitCounters.values()) {
                counter.set(0);
            }
            
            recordTransition(oldState, currentState, StateEvent.RESET);
            notifyStateChange(oldState, currentState, StateEvent.RESET);
        } finally {
            stateLock.unlock();
        }
    }
    
    /**
     * State transition record
     */
    public static class StateTransition {
        public final ConnectionState fromState;
        public final ConnectionState toState;
        public final StateEvent event;
        public final long timestamp;
        
        public StateTransition(ConnectionState fromState, ConnectionState toState, StateEvent event, long timestamp) {
            this.fromState = fromState;
            this.toState = toState;
            this.event = event;
            this.timestamp = timestamp;
        }
    }
    
    /**
     * State change listener interface
     */
    public interface StateChangeListener {
        void onStateChange(int connectionId, ConnectionState oldState, ConnectionState newState, StateEvent event);
    }
}
