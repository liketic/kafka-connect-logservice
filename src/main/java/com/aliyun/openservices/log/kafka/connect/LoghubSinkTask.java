package com.aliyun.openservices.log.kafka.connect;

import com.aliyun.openservices.aliyun.log.producer.LogProducer;
import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.aliyun.log.producer.ProjectConfig;
import com.aliyun.openservices.aliyun.log.producer.Result;
import com.aliyun.openservices.log.common.LogItem;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


public class LoghubSinkTask extends SinkTask {

    private static Logger log = LoggerFactory.getLogger(LoghubSinkTask.class);

    private LoghubSinkConnectorConfig config;
    private Producer producer;
    private int batchSize;
    private RecordConverter converter;

    private static Producer createProducer(LoghubSinkConnectorConfig config) {
        ProducerConfig producerConfig = new ProducerConfig();
        Producer producer = new LogProducer(producerConfig);
        ProjectConfig projectConfig = new ProjectConfig(
                config.getProject(),
                config.getEndpoint(),
                config.getAccessKeyId(),
                config.getAccessKeySecret());
        producer.putProjectConfig(projectConfig);
        return producer;
    }

    private static RecordConverter createConverter(LoghubSinkConnectorConfig config) {
        if ("raw".equalsIgnoreCase(config.getFormat())) {
            return new RawRecordConverter();
        } else if ("json".equalsIgnoreCase(config.getFormat())) {
            return new JsonRecordConverter();
        } else {
            return new RawRecordConverter();
        }
    }

    @Override
    public void start(Map<String, String> settings) {
        this.config = new LoghubSinkConnectorConfig(settings);
        this.producer = createProducer(config);
        this.batchSize = config.getBatchSize();
        this.converter = createConverter(config);
    }

    private LogItem convertRecord(SinkRecord record) {
        try {
            LogItem logItem = converter.convert(record);
            if (logItem == null) {
                return null;
            }
            logItem.PushBack("kafka_topic", record.topic());
            logItem.PushBack("kafka_offset", String.valueOf(record.kafkaOffset()));
            logItem.PushBack("kafka_timestamp", String.valueOf(record.timestamp()));
            logItem.PushBack("kafka_partition", String.valueOf(record.kafkaPartition()));
            return logItem;
        } catch (Exception ex) {
            log.error("Error parsing record {}", record.value(), ex);
        }
        return null;
    }
    private void putAsync(Collection<SinkRecord> records){
        List<LogItem> logs = new ArrayList<>(batchSize);
        List<Future<Result>> futures = new ArrayList<>();
        for (SinkRecord record : records) {
            LogItem item = convertRecord(record);
            if (item == null) {
                continue;
            }
            logs.add(item);
            if (logs.size() >= batchSize) {
                futures.add(send(logs));
                logs = new ArrayList<>();
            }
        }
        if (!logs.isEmpty()) {
            futures.add(send(logs));
        }
        for (Future<Result> future : futures) {
            if (future != null) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    log.error("Interrupted exception", e);
                } catch (ExecutionException e) {
                    log.error("Error on sending records", e.getCause());
                }
            }
        }
        futures.clear();
    }

    private void putSync(Collection<SinkRecord> records){
        List<LogItem> logs = new ArrayList<>(batchSize);
        for (SinkRecord record : records) {
            LogItem item = convertRecord(record);
            if (item == null) {
                continue;
            }
            logs.add(item);
            if (logs.size() >= batchSize) {
                try {
                    send(logs).get();
                } catch (InterruptedException e) {
                    log.error("Interrupted exception", e);
                } catch (ExecutionException e) {
                    log.error("Error on sending records", e.getCause());
                }
            }
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (config.getOrderingEnabled()){
            putSync(records);
        }else{
            putAsync(records);
        }
    }

    private Future<Result> send(List<LogItem> records) {
        try {
            return producer.send(config.getProject(), config.getLogstore(), records);
        } catch (Exception ex) {
            throw new ConnectException(ex);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    }

    @Override
    public void stop() {
        try {
            producer.close();
        } catch (Throwable e) {
            log.error("Error closing producer", e);
        }
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
