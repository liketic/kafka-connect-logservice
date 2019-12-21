package com.aliyun.openservices.log.kafka.connect;

import com.aliyun.openservices.log.common.LogItem;
import org.apache.kafka.connect.sink.SinkRecord;

public interface RecordConverter {

    /**
     * Convert a {@code SinkRecord} to {@code LogItem}
     *
     * @param record Kafka sink record
     * @return Loghub log item
     */
    LogItem convert(SinkRecord record);
}
