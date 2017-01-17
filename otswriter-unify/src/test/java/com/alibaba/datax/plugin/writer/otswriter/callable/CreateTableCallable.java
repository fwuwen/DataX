package com.alibaba.datax.plugin.writer.otswriter.callable;

import java.util.concurrent.Callable;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.CreateTableResponse;

public class CreateTableCallable implements Callable<CreateTableResponse>{

    private SyncClient ots = null;
    private CreateTableRequest createTableRequest = null;
    
    public CreateTableCallable(SyncClient ots, CreateTableRequest createTableRequest) {
        this.ots = ots;
        this.createTableRequest = createTableRequest;
    }
    
    @Override
    public CreateTableResponse call() throws Exception {
        return ots.createTable(createTableRequest);
    }
}
