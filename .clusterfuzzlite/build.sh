#!/bin/bash -eu
# Copyright 2024 AetherFlow Project
# Licensed under the Apache License, Version 2.0

# Define write_jvm_fuzzer_wrapper if not already defined
if ! declare -f write_jvm_fuzzer_wrapper > /dev/null; then
  echo "Defining custom write_jvm_fuzzer_wrapper..."
  write_jvm_fuzzer_wrapper() {
    local fuzzer_name=$1
    local target_class=$2
    local extra_classpath=$3
    
    # Copy jazzer_driver if not already in $OUT
    if [ ! -f "$OUT/jazzer_driver" ]; then
      if [ -f "/usr/local/bin/jazzer_driver" ]; then
        cp "/usr/local/bin/jazzer_driver" "$OUT/"
      elif which jazzer_driver > /dev/null 2>&1; then
        cp "$(which jazzer_driver)" "$OUT/"
      fi
    fi
    
    # Copy jazzer_agent_deploy.jar if not already in $OUT
    if [ ! -f "$OUT/jazzer_agent_deploy.jar" ]; then
      if [ -f "/usr/local/lib/jazzer_agent_deploy.jar" ]; then
        cp "/usr/local/lib/jazzer_agent_deploy.jar" "$OUT/"
      elif [ -n "${JAZZER_AGENT_PATH:-}" ] && [ -f "$JAZZER_AGENT_PATH" ]; then
        cp "$JAZZER_AGENT_PATH" "$OUT/jazzer_agent_deploy.jar"
      fi
    fi

    # Create the wrapper script
    echo "#!/bin/sh
# LLVMFuzzerTestOneInput for fuzzer detection.
this_dir=\$(dirname \"\$0\")
if [ -f \"\$this_dir/jazzer_agent_deploy.jar\" ]; then
  agent_path=\"\$this_dir/jazzer_agent_deploy.jar\"
else
  agent_path=\"/usr/local/lib/jazzer_agent_deploy.jar\"
fi

if [ -f \"\$this_dir/jazzer_driver\" ]; then
  driver_path=\"\$this_dir/jazzer_driver\"
else
  driver_path=\"jazzer_driver\"
fi

exec \"\$driver_path\" \\
  --agent_path=\"\$agent_path\" \\
  --cp=\"$extra_classpath\" \\
  --target_class=\"$target_class\" \\
  \"\$@\"" > "$OUT/$fuzzer_name"
    
    chmod +x "$OUT/$fuzzer_name"
  }
fi

# Navigate to project root
cd "$SRC"

# Create classes directory
mkdir -p "$OUT/classes"

# Compile all source files (AetherFlow)
echo "Compiling AetherFlow..."
javac -d "$OUT/classes" \
    -source 11 \
    -target 11 \
    $(find src/main/java -name "*.java")

# Compile all fuzz files using the compiled main classes on classpath
echo "Compiling fuzz harnesses..."
javac -d "$OUT/classes" \
    -source 11 \
    -target 11 \
    -cp "$OUT/classes" \
    $(find fuzz -name "*.java")

# Package both compiled source and fuzzers into a single self-contained JAR
echo "Creating fuzzer JAR..."
cd "$OUT/classes"
jar cf "$OUT/aether-flow-fuzzer.jar" .

# Create the wrapper scripts for each fuzzer
# Syntax: write_jvm_fuzzer_wrapper <fuzzer_executable_name> <fully_qualified_target_class> [extra_classpath]
write_jvm_fuzzer_wrapper ConnectionPoolFuzzer com.aetherflow.ConnectionPoolFuzzer "\$this_dir/aether-flow-fuzzer.jar"
write_jvm_fuzzer_wrapper PacketAssemblerFuzzer com.aetherflow.PacketAssemblerFuzzer "\$this_dir/aether-flow-fuzzer.jar"
write_jvm_fuzzer_wrapper ProtocolParserFuzzer com.aetherflow.ProtocolParserFuzzer "\$this_dir/aether-flow-fuzzer.jar"
write_jvm_fuzzer_wrapper SessionManagerFuzzer com.aetherflow.SessionManagerFuzzer "\$this_dir/aether-flow-fuzzer.jar"
write_jvm_fuzzer_wrapper StateMachineFuzzer com.aetherflow.StateMachineFuzzer "\$this_dir/aether-flow-fuzzer.jar"

# Ensure executability
chmod +x "$OUT/ConnectionPoolFuzzer"
chmod +x "$OUT/PacketAssemblerFuzzer"
chmod +x "$OUT/ProtocolParserFuzzer"
chmod +x "$OUT/SessionManagerFuzzer"
chmod +x "$OUT/StateMachineFuzzer"
