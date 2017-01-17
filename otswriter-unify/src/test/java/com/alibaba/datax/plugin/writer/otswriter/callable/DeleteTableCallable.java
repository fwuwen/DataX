package com.alibaba.datax.plugin.writer.otswriter.callable;

import java.util.concurrent.Callable;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.DeleteTableResponse;

public class DeleteTableCallable implements Callable<DeleteTableResponse>{

    private SyncClient ots = null;
    private DeleteTableRequest deleteTableRequest = null;
    
    public DeleteTableCallable(SyncClient ots, DeleteTableRequest deleteTableRequest) {
        this.ots = ots;
        this.deleteTableRequest = deleteTableRequest;
    }
    
    @Override
    public DeleteTableResponse call() throws Exception {
        return ots.deleteTable(deleteTableRequest);
    }
}
