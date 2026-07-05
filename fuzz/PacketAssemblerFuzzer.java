package com.aetherflow;

import com.aetherflow.PacketAssembler;
import com.aetherflow.PacketAssembler.Fragment;

/**
 * Jazzer fuzzing harness for packet assembler.
 * Tests packet reassembly with malformed fragment data.
 */
public class PacketAssemblerFuzzer {
    
    private static PacketAssembler packetAssembler;
    
    static {
        packetAssembler = new PacketAssembler();
    }
    
    /**
     * Fuzzer entry point for packet assembly
     */
    public static void fuzzerTestOneInput(byte[] data) {
        try {
            if (packetAssembler == null || data == null || data.length < 8) {
                return;
            }
            
            // Parse data as fragment
            int offset = 0;
            
            // Extract sequence number (4 bytes)
            int sequenceNumber = ((data[offset] & 0xFF) << 24) |
                                ((data[offset + 1] & 0xFF) << 16) |
                                ((data[offset + 2] & 0xFF) << 8) |
                                (data[offset + 3] & 0xFF);
            offset += 4;
            
            // Extract fragment index (2 bytes)
            int fragmentIndex = ((data[offset] & 0xFF) << 8) |
                               (data[offset + 1] & 0xFF);
            offset += 2;
            
            // Extract total fragments (2 bytes)
            int totalFragments = ((data[offset] & 0xFF) << 8) |
                               (data[offset + 1] & 0xFF);
            offset += 2;
            
            // Extract fragment offset (4 bytes)
            int fragmentOffset = ((data[offset] & 0xFF) << 24) |
                                ((data[offset + 1] & 0xFF) << 16) |
                                ((data[offset + 2] & 0xFF) << 8) |
                                (data[offset + 3] & 0xFF);
            offset += 4;
            
            // Extract total size (4 bytes)
            int totalSize = ((data[offset] & 0xFF) << 24) |
                           ((data[offset + 1] & 0xFF) << 16) |
                           ((data[offset + 2] & 0xFF) << 8) |
                           (data[offset + 3] & 0xFF);
            offset += 4;
            
            // Remaining data is fragment payload
            byte[] fragmentData = new byte[data.length - offset];
            System.arraycopy(data, offset, fragmentData, 0, fragmentData.length);
            
            // Create fragment
            Fragment fragment = new Fragment(
                sequenceNumber,
                fragmentIndex,
                totalFragments,
                fragmentOffset,
                totalSize,
                fragmentData
            );
            
            // Add fragment to assembler
            PacketAssembler.AssemblerResult result = packetAssembler.addFragment(fragment);
            
            if (result.success && result.data != null) {
                // Process assembled data
                byte[] assembled = result.data;
                
                // Access assembled bytes
                for (int i = 0; i < Math.min(assembled.length, 100); i++) {
                    byte b = assembled[i];
                }
            }
            
            // Get reassembly context
            PacketAssembler.ReassemblyContext context = packetAssembler.getContext(sequenceNumber);
            if (context != null) {
                context.getReceivedFragmentCount();
                context.getMissingFragmentCount();
                context.getMissingFragmentIndices();
                context.isComplete();
                context.getAge();
                context.getIdleTime();
            }
            
            // Get statistics
            packetAssembler.getStats();
            
            // Try fragmenting a packet
            if (data.length >= 16) {
                byte[] testData = new byte[Math.min(data.length - 16, 1024)];
                System.arraycopy(data, 16, testData, 0, testData.length);
                packetAssembler.fragmentPacket(sequenceNumber + 1, testData, 256);
            }
            
            // Cleanup expired contexts
            packetAssembler.cleanupExpiredContexts();
            
        } catch (Exception e) {
            // Ignore exceptions during fuzzing
        }
    }
}
