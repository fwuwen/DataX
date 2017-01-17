package com.alibaba.datax.plugin.writer.otswriter.model;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.otswriter.OTSCriticalException;
import com.alibaba.datax.plugin.writer.otswriter.utils.CalculateHelper;
import com.alicloud.openservices.tablestore.model.*;

public class OTSLine {
    private int dataSize = 0;

    private PrimaryKey pk = null;
    private RowChange change = null;
    
    private List<Record> records = new ArrayList<Record>();
    
    public OTSLine(
            PrimaryKey pk,
            List<Record> records,
            RowChange change) throws OTSCriticalException {
        this.pk = pk;
        this.change = change;
        this.records.addAll(records);
        setSize(this.change);
    }
    
    public OTSLine(
            PrimaryKey pk,
            Record record,
            RowChange change) throws OTSCriticalException {
        this.pk = pk;
        this.change = change;
        this.records.add(record);
        setSize(this.change);
    }
    
    private void setSize(RowChange change) throws OTSCriticalException {
        if (change instanceof RowPutChange) {
            this.dataSize = CalculateHelper.getRowPutChangeSize((RowPutChange) change);
        } else if (change instanceof RowUpdateChange) {
            this.dataSize = CalculateHelper.getRowUpdateChangeSize((RowUpdateChange) change);
        } else {
            throw new RuntimeException(String.format(OTSErrorMessage.UNSUPPORT_PARSE, change.getClass().toString(), "RowPutChange or RowUpdateChange"));
        }
    }
    
    public List<Record> getRecords() {
        return records;
    }

    public PrimaryKey getPk() {
        return pk;
    }

    public int getDataSize() {
        return dataSize;
    }

    public RowChange getRowChange() {
        return change;
    }
}
