package com.alibaba.datax.plugin.writer.otswriter.common;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;

public class OTSPrimaryKeyBuilder {
    
    private List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
    
    private OTSPrimaryKeyBuilder() {}
    
    public static OTSPrimaryKeyBuilder newInstance() {
        return new OTSPrimaryKeyBuilder();
    }
    
    public OTSPrimaryKeyBuilder add(String name, PrimaryKeyValue value) {
        primaryKeyColumn.add(new PrimaryKeyColumn(name, value));
        return this;
    }
    
    public PrimaryKey toPrimaryKey() {
        return new PrimaryKey(primaryKeyColumn);
    }
}
