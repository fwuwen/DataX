package com.aliyun.openservices.ots.internal.model;

import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;
import com.alicloud.openservices.tablestore.model.Response;

public class MockBatchWriteRowResult extends BatchWriteRowResponse {

    public MockBatchWriteRowResult(Response meta) {
        super(meta);
    }
 
    public void addRowResult(RowResult status) {
        super.addRowResult(status);
    }
}
