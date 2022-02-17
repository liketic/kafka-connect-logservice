package com.aliyun.openservices.log.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;


public class LoghubSinkConnectorConfig extends AbstractConfig {
    private static final String ENDPOINT_CONF = "loghub.endpoint";
    private static final String ACCESS_KEY_ID_CONF = "loghub.accessKeyId";
    private static final String ACCESS_KEY_SECRET_CONF = "loghub.accessKeySecret";
    private static final String PROJECT_CONF = "loghub.project";
    private static final String LOGSTORE_CONF = "loghub.logstore";
    private static final String BATCH_SIZE_CONF = "loghub.batchSize";
    private static final String ORDERING_ENABLED = "loghub.ordering.enabled";
    private static final String FORMAT_CONF = "format";

    private String endpoint;
    private String accessKeyId;
    private String accessKeySecret;
    private String project;
    private String logstore;
    private int batchSize;
    private String format;
    private Boolean orderingEnabled;

    public LoghubSinkConnectorConfig(Map<?, ?> originals) {
        super(config(), originals);
        endpoint = getString(ENDPOINT_CONF);
        accessKeyId = getString(ACCESS_KEY_ID_CONF);
        accessKeySecret = getString(ACCESS_KEY_SECRET_CONF);
        project = getString(PROJECT_CONF);
        logstore = getString(LOGSTORE_CONF);
        batchSize = getInt(BATCH_SIZE_CONF);
        format = getString(FORMAT_CONF);
        orderingEnabled = getBoolean(ORDERING_ENABLED);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(ENDPOINT_CONF,
                        Type.STRING,
                        Importance.HIGH,
                        "Loghub Endpoint")
                .define(ACCESS_KEY_ID_CONF,
                        Type.STRING,
                        Importance.HIGH,
                        "Loghub Access Key ID")
                .define(ACCESS_KEY_SECRET_CONF,
                        Type.STRING,
                        Importance.HIGH,
                        "Loghub Access Key Secret")
                .define(PROJECT_CONF,
                        Type.STRING,
                        Importance.HIGH,
                        "Loghub Project")
                .define(LOGSTORE_CONF,
                        Type.STRING,
                        Importance.HIGH,
                        "Loghub Logstore")
                .define(FORMAT_CONF,
                        Type.STRING,
                        Importance.HIGH,
                        "Record format, raw or json")
                .define(BATCH_SIZE_CONF,
                        Type.INT,
                        2000,
                        Importance.MEDIUM,
                        "Batch size")
                .define(ORDERING_ENABLED,
                        Type.BOOLEAN,
                        Importance.MEDIUM,
                        "Put records to SLS the same order as that polled from Kafka");
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    public String getProject() {
        return project;
    }

    public String getLogstore() {
        return logstore;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public String getFormat() {
        return format;
    }

    public Boolean getOrderingEnabled() {return orderingEnabled; }
}
