package com.aliyun.openservices.log.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class LoghubSinkConnector extends SinkConnector {

    private Map<String, String> props;

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>(props);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void start(Map<String, String> settings) {
        try {
            props = settings;
            new LoghubSinkConnectorConfig(settings);
        } catch (ConfigException e) {
            throw new ConnectException(
                    "Couldn't start LoghubSinkConnector due to configuration error",
                    e);
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return LoghubSinkConnectorConfig.config();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return LoghubSinkTask.class;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
