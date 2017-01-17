package com.alibaba.datax.plugin.writer.otswriter.callable;

import java.util.concurrent.Callable;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.*;

public class UpdateRowChangeCallable implements Callable<UpdateRowResponse>{
    
    private SyncClientInterface ots = null;
    private UpdateRowRequest updateRowRequest  = null;

    public UpdateRowChangeCallable(SyncClientInterface ots, UpdateRowRequest updateRowRequest ) {
        this.ots = ots;
        this.updateRowRequest = updateRowRequest;
    }
    
    @Override
    public UpdateRowResponse call() throws Exception {
        return ots.updateRow(updateRowRequest);
    }

}
