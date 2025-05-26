package com.dqs.eventdrivensearch.queryExecution.search.io;

public class EnvironmentVars {

    private static final String CLOUD_WATCH_NAMESPACE = "CLOUD_WATCH_NAMESPACE";
    private static final String OUTPUT_FOLDER_PATH = "OUTPUT_FOLDER_PATH";

    public static String getCloudwachNamespace() {
        String namespace = System.getenv(CLOUD_WATCH_NAMESPACE);
        return namespace != null ? namespace : "DQS_ARRAY_BATCH_JOB_METRICS";
    }

    public static String getOutPutFolderPath() {
        return System.getenv(OUTPUT_FOLDER_PATH);
    }
}

