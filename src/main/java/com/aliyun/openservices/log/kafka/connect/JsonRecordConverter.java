package com.aliyun.openservices.log.kafka.connect;

import com.aliyun.openservices.log.common.LogItem;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.util.Map;

public class JsonRecordConverter implements RecordConverter {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public LogItem convert(SinkRecord record) {
        try {
            Map<String, Object> jsonNode = objectMapper.readValue(record.value().toString(),
                    new TypeReference<Map<String, Object>>() {
                    });
            LogItem logItem = new LogItem();
            for (Map.Entry<String, Object> entry : jsonNode.entrySet()) {
                logItem.PushBack(entry.getKey(), String.valueOf(entry.getValue()));
            }
            return logItem;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
