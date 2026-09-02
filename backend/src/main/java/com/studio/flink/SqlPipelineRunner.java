package com.studio.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SqlPipelineRunner {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException("Pipeline termination: Missing path parameter pointing to generated Flink SQL script file.");
        }

        // 1. Initialize the Flink Cluster Execution Context
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. Read the live generated UI SQL script file
        String sqlFilePath = args[0];
        String sqlScript = new String(Files.readAllBytes(Paths.get(sqlFilePath)));

        // 3. Parse individual instructions separated by semicolons
        String[] statements = sqlScript.split(";");
        
        for (String statement : statements) {
            String sanitized = statement.trim();
            if (!sanitized.isEmpty()) {
                // Execute DDL schemas and pipeline insert rules sequentially
                tableEnv.executeSql(sanitized);
            }
        }
    }
}
