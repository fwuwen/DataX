package com.alibaba.datax.plugin.writer.otswriter.common;

import java.util.List;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;

public class TestRecordReceiver implements RecordReceiver {
    
    private List<Record> contents = null;

    public TestRecordReceiver(List<Record> contents) {
        this.contents = contents;
    }
    
    @Override
    public Record getFromReader() {
        if(!contents.isEmpty()){
            return contents.remove(0);
        }
        return null;
    }

    public void shutdown(){

    }
    
}
