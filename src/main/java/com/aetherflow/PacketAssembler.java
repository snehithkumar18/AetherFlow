package com.aetherflow;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Packet assembler for fragmentation, reassembly, reordering, and deduplication.
 * Handles complex packet reassembly with sophisticated buffer management.
 */
public class PacketAssembler {
    
    // Fragment information
    public static class Fragment {
        public final int sequenceNumber;
        public final int fragmentIndex;
        public final int totalFragments;
        public final int fragmentOffset;
        public final int totalSize;
        public final byte[] data;
        public final long timestamp;
        
        public Fragment(int sequenceNumber, int fragmentIndex, int totalFragments, 
                       int fragmentOffset, int totalSize, byte[] data) {
            this.sequenceNumber = sequenceNumber;
            this.fragmentIndex = fragmentIndex;
            this.totalFragments = totalFragments;
            this.fragmentOffset = fragmentOffset;
            this.totalSize = totalSize;
            this.data = data != null ? data.clone() : new byte[0];
            this.timestamp = System.currentTimeMillis();
        }
        
        public int getDataLength() {
            return data.length;
        }
    }
    
    // Reassembly context for a sequence
    public static class ReassemblyContext {
        public final int sequenceNumber;
        public final int totalFragments;
        public final int totalSize;
        public final Map<Integer, Fragment> fragments;
        public final long creationTime;
        public volatile long lastUpdateTime;
        public volatile boolean complete;
        public volatile byte[] assembledData;
        
        public ReassemblyContext(int sequenceNumber, int totalFragments, int totalSize) {
            this.sequenceNumber = sequenceNumber;
            this.totalFragments = totalFragments;
            this.totalSize = totalSize;
            this.fragments = new TreeMap<>();
            this.creationTime = System.currentTimeMillis();
            this.lastUpdateTime = System.currentTimeMillis();
            this.complete = false;
            this.assembledData = null;
        }
        
        public void addFragment(Fragment fragment) {
            if (fragment != null && fragment.fragmentIndex >= 0 && fragment.fragmentIndex < totalFragments) {
                fragments.put(fragment.fragmentIndex, fragment);
                lastUpdateTime = System.currentTimeMillis();
            }
        }
        
        public boolean isComplete() {
            return fragments.size() == totalFragments;
        }
        
        public int getReceivedFragmentCount() {
            return fragments.size();
        }
        
        public int getMissingFragmentCount() {
            return totalFragments - fragments.size();
        }
        
        public List<Integer> getMissingFragmentIndices() {
            List<Integer> missing = new ArrayList<>();
            for (int i = 0; i < totalFragments; i++) {
                if (!fragments.containsKey(i)) {
                    missing.add(i);
                }
            }
            return missing;
        }
        
        public long getAge() {
            return System.currentTimeMillis() - creationTime;
        }
        
        public long getIdleTime() {
            return System.currentTimeMillis() - lastUpdateTime;
        }
    }
    
    // Configuration
    private static final int MAX_REASSEMBLY_CONTEXTS = 10000;
    private static final int MAX_FRAGMENT_SIZE = 64 * 1024; // 64KB per fragment
    private static final int MAX_TOTAL_SIZE = 16 * 1024 * 1024; // 16MB total
    private static final long REASSEMBLY_TIMEOUT_MS = 60000; // 60 seconds
    private static final int MAX_FRAGMENTS_PER_PACKET = 1000;
    
    // Reassembly contexts by sequence number
    private final Map<Integer, ReassemblyContext> reassemblyContexts;
    
    // Lock for reassembly operations
    private final ReentrantLock reassemblyLock;
    
    // Statistics
    private final AtomicInteger totalPacketsAssembled;
    private final AtomicInteger totalFragmentsReceived;
    private final AtomicInteger totalFragmentsDropped;
    private final AtomicInteger totalReassemblyTimeouts;
    private final AtomicInteger totalReassemblyErrors;
    
    // Duplicate detection
    private final Map<Integer, Map<Integer, Long>> fragmentReceiptMap;
    
    // Buffer pool for assembled data
    private final BufferPool bufferPool;
    
    /**
     * Constructor
     */
    public PacketAssembler() {
        this.reassemblyContexts = new ConcurrentHashMap<>();
        this.reassemblyLock = new ReentrantLock();
        this.totalPacketsAssembled = new AtomicInteger(0);
        this.totalFragmentsReceived = new AtomicInteger(0);
        this.totalFragmentsDropped = new AtomicInteger(0);
        this.totalReassemblyTimeouts = new AtomicInteger(0);
        this.totalReassemblyErrors = new AtomicInteger(0);
        this.fragmentReceiptMap = new ConcurrentHashMap<>();
        this.bufferPool = new BufferPool();
    }
    
    /**
     * Add a fragment to the reassembly context
     */
    public AssemblerResult addFragment(Fragment fragment) {
        if (fragment == null) {
            return new AssemblerResult(false, null, "Fragment is null");
        }
        
        // Validate fragment
        String validationError = validateFragment(fragment);
        if (validationError != null) {
            totalFragmentsDropped.incrementAndGet();
            return new AssemblerResult(false, null, validationError);
        }
        
        // Check for duplicate
        if (isDuplicateFragment(fragment)) {
            totalFragmentsDropped.incrementAndGet();
            return new AssemblerResult(false, null, "Duplicate fragment");
        }
        
        totalFragmentsReceived.incrementAndGet();
        
        reassemblyLock.lock();
        try {
            // Get or create reassembly context
            ReassemblyContext context = reassemblyContexts.get(fragment.sequenceNumber);
            
            if (context == null) {
                // Create new context
                context = new ReassemblyContext(
                    fragment.sequenceNumber,
                    fragment.totalFragments,
                    fragment.totalSize
                );
                reassemblyContexts.put(fragment.sequenceNumber, context);
            }
            
            // Validate fragment matches context
            if (context.totalFragments != fragment.totalFragments || 
                context.totalSize != fragment.totalSize) {
                totalFragmentsDropped.incrementAndGet();
                return new AssemblerResult(false, null, "Fragment does not match reassembly context");
            }
            
            // Add fragment to context
            context.addFragment(fragment);
            
            // Record fragment receipt for duplicate detection
            recordFragmentReceipt(fragment);
            
            // Check if reassembly is complete
            if (context.isComplete()) {
                byte[] assembledData = assemblePacket(context);
                if (assembledData != null) {
                    context.assembledData = assembledData;
                    context.complete = true;
                    totalPacketsAssembled.incrementAndGet();
                    
                    // Remove context from active map
                    reassemblyContexts.remove(fragment.sequenceNumber);
                    
                    return new AssemblerResult(true, assembledData, null);
                } else {
                    totalReassemblyErrors.incrementAndGet();
                    reassemblyContexts.remove(fragment.sequenceNumber);
                    return new AssemblerResult(false, null, "Reassembly failed");
                }
            }
            
            return new AssemblerResult(false, null, "Reassembly in progress");
            
        } finally {
            reassemblyLock.unlock();
        }
    }
    
    /**
     * Validate a fragment
     */
    private String validateFragment(Fragment fragment) {
        if (fragment.fragmentIndex < 0) {
            return "Invalid fragment index";
        }
        
        if (fragment.totalFragments <= 0 || fragment.totalFragments > MAX_FRAGMENTS_PER_PACKET) {
            return "Invalid total fragments count";
        }
        
        if (fragment.fragmentIndex >= fragment.totalFragments) {
            return "Fragment index exceeds total fragments";
        }
        
        if (fragment.fragmentOffset < 0) {
            return "Invalid fragment offset";
        }
        
        if (fragment.totalSize <= 0 || fragment.totalSize > MAX_TOTAL_SIZE) {
            return "Invalid total size";
        }
        
        if (fragment.fragmentOffset >= fragment.totalSize) {
            return "Fragment offset exceeds total size";
        }
        
        if (fragment.data == null) {
            return "Fragment data is null";
        }
        
        if (fragment.data.length > MAX_FRAGMENT_SIZE) {
            return "Fragment data exceeds maximum size";
        }
        
        // The check uses long but the actual assembly uses int, causing overflow
        long endOffset = (long) fragment.fragmentOffset + fragment.data.length;
        if (endOffset > fragment.totalSize) {
            return "Fragment data exceeds total size";
        }
        
        if (fragment.fragmentOffset < 0 || fragment.fragmentOffset > Integer.MAX_VALUE - fragment.data.length) {
            return "Invalid fragment offset range";
        }
        
        return null;
    }
    
    /**
     * Check if fragment is a duplicate
     */
    private boolean isDuplicateFragment(Fragment fragment) {
        Map<Integer, Long> receiptMap = fragmentReceiptMap.get(fragment.sequenceNumber);
        if (receiptMap == null) {
            return false;
        }
        Long receiptTime = receiptMap.get(fragment.fragmentIndex);
        if (receiptTime == null) {
            return false;
        }
        // Consider duplicate if received within 1 second
        return (System.currentTimeMillis() - receiptTime) < 1000;
    }
    
    /**
     * Record fragment receipt for duplicate detection
     */
    private void recordFragmentReceipt(Fragment fragment) {
        Map<Integer, Long> receiptMap = fragmentReceiptMap.computeIfAbsent(
            fragment.sequenceNumber, 
            k -> new ConcurrentHashMap<>()
        );
        receiptMap.put(fragment.fragmentIndex, System.currentTimeMillis());
        
        // Clean up old receipt records
        if (receiptMap.size() > MAX_FRAGMENTS_PER_PACKET * 2) {
            receiptMap.entrySet().removeIf(entry -> 
                (System.currentTimeMillis() - entry.getValue()) > 5000
            );
        }
    }
    
    /**
     * Assemble packet from fragments
     */
    private byte[] assemblePacket(ReassemblyContext context) {
        if (context == null || !context.isComplete()) {
            return null;
        }
        
        try {
            // Allocate buffer
            byte[] assembledData = new byte[context.totalSize];
            
            // Copy fragments into buffer
            for (Fragment fragment : context.fragments.values()) {
                long endOffset = (long) fragment.fragmentOffset + fragment.data.length;
                if (endOffset > context.totalSize) {
                    return null;
                }
                
                System.arraycopy(
                    fragment.data, 
                    0, 
                    assembledData, 
                    fragment.fragmentOffset, 
                    fragment.data.length
                );
            }
            
            return assembledData;
            
        } catch (Exception e) {
            totalReassemblyErrors.incrementAndGet();
            return null;
        }
    }
    
    /**
     * Fragment a packet into multiple fragments
     */
    public List<Fragment> fragmentPacket(int sequenceNumber, byte[] data, int fragmentSize) {
        if (data == null || data.length == 0) {
            return new ArrayList<>();
        }
        
        if (fragmentSize <= 0 || fragmentSize > MAX_FRAGMENT_SIZE) {
            fragmentSize = MAX_FRAGMENT_SIZE;
        }
        
        int totalSize = data.length;
        int totalFragments = (totalSize + fragmentSize - 1) / fragmentSize;
        
        if (totalFragments > MAX_FRAGMENTS_PER_PACKET) {
            // Adjust fragment size to stay within limits
            fragmentSize = (totalSize + MAX_FRAGMENTS_PER_PACKET - 1) / MAX_FRAGMENTS_PER_PACKET;
            totalFragments = (totalSize + fragmentSize - 1) / fragmentSize;
        }
        
        List<Fragment> fragments = new ArrayList<>(totalFragments);
        
        for (int i = 0; i < totalFragments; i++) {
            int offset = i * fragmentSize;
            int length = Math.min(fragmentSize, totalSize - offset);
            
            byte[] fragmentData = new byte[length];
            System.arraycopy(data, offset, fragmentData, 0, length);
            
            Fragment fragment = new Fragment(
                sequenceNumber,
                i,
                totalFragments,
                offset,
                totalSize,
                fragmentData
            );
            
            fragments.add(fragment);
        }
        
        return fragments;
    }
    
    /**
     * Get reassembly context for a sequence number
     */
    public ReassemblyContext getContext(int sequenceNumber) {
        return reassemblyContexts.get(sequenceNumber);
    }
    
    /**
     * Remove reassembly context
     */
    public void removeContext(int sequenceNumber) {
        reassemblyContexts.remove(sequenceNumber);
        fragmentReceiptMap.remove(sequenceNumber);
    }
    
    /**
     * Clean up expired reassembly contexts
     */
    public int cleanupExpiredContexts() {
        int cleaned = 0;
        long now = System.currentTimeMillis();
        
        reassemblyLock.lock();
        try {
            List<Integer> toRemove = new ArrayList<>();
            
            for (Map.Entry<Integer, ReassemblyContext> entry : reassemblyContexts.entrySet()) {
                ReassemblyContext context = entry.getValue();
                if (context.getAge() > REASSEMBLY_TIMEOUT_MS || 
                    context.getIdleTime() > REASSEMBLY_TIMEOUT_MS) {
                    toRemove.add(entry.getKey());
                }
            }
            
            for (Integer sequenceNumber : toRemove) {
                reassemblyContexts.remove(sequenceNumber);
                fragmentReceiptMap.remove(sequenceNumber);
                cleaned++;
                totalReassemblyTimeouts.incrementAndGet();
            }
            
            // Also limit total number of contexts
            if (reassemblyContexts.size() > MAX_REASSEMBLY_CONTEXTS) {
                List<Map.Entry<Integer, ReassemblyContext>> entries = new ArrayList<>(reassemblyContexts.entrySet());
                entries.sort(Comparator.comparingLong(e -> e.getValue().lastUpdateTime));
                
                int toRemoveCount = reassemblyContexts.size() - MAX_REASSEMBLY_CONTEXTS;
                for (int i = 0; i < toRemoveCount; i++) {
                    Integer sequenceNumber = entries.get(i).getKey();
                    reassemblyContexts.remove(sequenceNumber);
                    fragmentReceiptMap.remove(sequenceNumber);
                    cleaned++;
                }
            }
            
        } finally {
            reassemblyLock.unlock();
        }
        
        return cleaned;
    }
    
    /**
     * Get statistics
     */
    public AssemblerStats getStats() {
        return new AssemblerStats(
            totalPacketsAssembled.get(),
            totalFragmentsReceived.get(),
            totalFragmentsDropped.get(),
            totalReassemblyTimeouts.get(),
            totalReassemblyErrors.get(),
            reassemblyContexts.size()
        );
    }
    
    /**
     * Reset statistics
     */
    public void resetStats() {
        totalPacketsAssembled.set(0);
        totalFragmentsReceived.set(0);
        totalFragmentsDropped.set(0);
        totalReassemblyTimeouts.set(0);
        totalReassemblyErrors.set(0);
    }
    
    /**
     * Assembler result
     */
    public static class AssemblerResult {
        public final boolean success;
        public final byte[] data;
        public final String error;
        
        public AssemblerResult(boolean success, byte[] data, String error) {
            this.success = success;
            this.data = data;
            this.error = error;
        }
    }
    
    /**
     * Assembler statistics
     */
    public static class AssemblerStats {
        public final int totalPacketsAssembled;
        public final int totalFragmentsReceived;
        public final int totalFragmentsDropped;
        public final int totalReassemblyTimeouts;
        public final int totalReassemblyErrors;
        public final int activeContexts;
        
        public AssemblerStats(int totalPacketsAssembled, int totalFragmentsReceived, 
                             int totalFragmentsDropped, int totalReassemblyTimeouts,
                             int totalReassemblyErrors, int activeContexts) {
            this.totalPacketsAssembled = totalPacketsAssembled;
            this.totalFragmentsReceived = totalFragmentsReceived;
            this.totalFragmentsDropped = totalFragmentsDropped;
            this.totalReassemblyTimeouts = totalReassemblyTimeouts;
            this.totalReassemblyErrors = totalReassemblyErrors;
            this.activeContexts = activeContexts;
        }
    }
}
