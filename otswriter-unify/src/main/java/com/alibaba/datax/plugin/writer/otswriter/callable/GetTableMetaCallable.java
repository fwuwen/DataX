package com.alibaba.datax.plugin.writer.otswriter.callable;

import java.util.concurrent.Callable;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.*;

public class GetTableMetaCallable implements Callable<TableMeta>{

    private SyncClientInterface ots = null;
    private String tableName = null;
    
    public GetTableMetaCallable(SyncClientInterface ots, String tableName) {
        this.ots = ots;
        this.tableName = tableName;
    }
    
    @Override
    public TableMeta call() throws Exception {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        DescribeTableResponse result = ots.describeTable(describeTableRequest);
        TableMeta tableMeta = result.getTableMeta();
        return tableMeta;
    }

}
