package com.aliyun.openservices.log.kafka.connect;

import com.aliyun.openservices.log.common.LogItem;
import org.apache.kafka.connect.sink.SinkRecord;

public class RawRecordConverter implements RecordConverter {

    @Override
    public LogItem convert(SinkRecord record) {
        LogItem logItem = new LogItem();
        logItem.PushBack("value", record.value().toString());
        return logItem;
    }
}
