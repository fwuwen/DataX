package com.alibaba.datax.plugin.writer.otswriter.common;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.Row;

public class OTSRowBuilder {
    
    private List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
    private List<Column> attrs = new ArrayList<Column>();
    
    private OTSRowBuilder() {}
    
    public static OTSRowBuilder newInstance() {
        return new OTSRowBuilder();
    }
    
    public OTSRowBuilder addPrimaryKeyColumn(String name, PrimaryKeyValue value) {
        primaryKeyColumn.add(new PrimaryKeyColumn(name, value));
        return this;
    }
    
//    public OTSRowBuilder addAttrColumn(String name, ColumnValue value) {
//        attrs.add(new Column(name, value));
//        return this;
//    }
    
    public OTSRowBuilder addAttrColumn(String name, ColumnValue value, long ts) {
        attrs.add(new Column(name, value, ts));
        return this;
    }
    
    public Row toRow() {
        return new Row(new PrimaryKey(primaryKeyColumn), attrs);
    }
}
