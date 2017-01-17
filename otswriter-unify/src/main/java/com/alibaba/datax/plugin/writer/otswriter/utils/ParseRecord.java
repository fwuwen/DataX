package com.alibaba.datax.plugin.writer.otswriter.utils;

import java.util.List;
import java.util.Map;

import com.alibaba.datax.plugin.writer.otswriter.model.*;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.otswriter.OTSCriticalException;
import com.alicloud.openservices.tablestore.model.*;

public class ParseRecord {
    
    private static final Logger LOG = LoggerFactory.getLogger(ParseRecord.class);
    
    private static com.alicloud.openservices.tablestore.model.Column buildColumn(String name, ColumnValue value, long timestamp) {
        if (timestamp > 0) {
            return new com.alicloud.openservices.tablestore.model.Column(
                    name, 
                    value,
                    timestamp
                    );
        } else {
            return new com.alicloud.openservices.tablestore.model.Column(
                    name, 
                    value
                    );
        }
    }
    /**
     * 基于普通方式处理Record
     * 当PK或者Attr解析失败时，方法会返回null
     * @param tableName
     * @param type
     * @param pkColumns
     * @param attrColumns
     * @param record
     * @param timestamp
     * @return
     * @throws OTSCriticalException 
     */
    public static OTSLine parseNormalRecordToOTSLine(
            String tableName, 
            OTSOpType type, 
            Map<TablePrimaryKeySchema, Integer> pkColumns,
            List<OTSAttrColumn> attrColumns,
            Record record,
            long timestamp) throws OTSCriticalException {
        
        PrimaryKey pk = Common.getPKFromRecord(pkColumns, record);
        if (pk == null) {
            return null;
        }
        List<Pair<String, ColumnValue>> values = Common.getAttrFromRecord(pkColumns.size(), attrColumns, record);
        if (values == null) {
            return null;
        }
        
        switch (type) {
            case PUT_ROW:
                RowPutChange rowPutChange = new RowPutChange(tableName, pk);
                for (Pair<String, ColumnValue> en : values) {
                    if (en.getSecond() != null) {
                        rowPutChange.addColumn(buildColumn(en.getFirst(), en.getSecond(), timestamp));
                    } 
                }
                if (rowPutChange.getColumnsToPut().isEmpty()) {
                	return null;
                } 
                return new OTSLine(pk, record, rowPutChange);
            case UPDATE_ROW:
                RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, pk);
                for (Pair<String, ColumnValue> en : values) {
                    if (en.getSecond() != null) {
                        rowUpdateChange.put(buildColumn(en.getFirst(), en.getSecond(), timestamp));
                    } else {
                        rowUpdateChange.deleteColumns(en.getFirst()); // 删除整列
                    }
                }
                return new OTSLine(pk, record, rowUpdateChange);
            default:
                LOG.error("Bug branch, can not support : {}(OTSOpType)", type);
                throw new OTSCriticalException(String.format(OTSErrorMessage.UNSUPPORT, type));
        }
    }
    
    public static String getDefineCoumnName(String attrColumnNamePrefixFilter, int columnNameIndex, Record r) {
        String columnName = r.getColumn(columnNameIndex).asString();
        if (attrColumnNamePrefixFilter != null) {
            if (columnName.startsWith(attrColumnNamePrefixFilter) && columnName.length() > attrColumnNamePrefixFilter.length()) {
                columnName = columnName.substring(attrColumnNamePrefixFilter.length());
            } else {
                throw new IllegalArgumentException(String.format(OTSErrorMessage.COLUMN_NOT_DEFINE, columnName));
            }
        } 
        return columnName;
    }
    
    private static void appendCellToRowUpdateChange(
            Map<TablePrimaryKeySchema, Integer> pkColumns,
            String attrColumnNamePrefixFilter,
            Record r,
            RowUpdateChange updateChange
            ) throws OTSCriticalException {
        try {
            String columnName = getDefineCoumnName(attrColumnNamePrefixFilter, pkColumns.size(), r);
            Column timestamp = r.getColumn(pkColumns.size() + 1);
            Column value = r.getColumn(pkColumns.size() + 2);
            
            if (timestamp.getRawData() == null) {
                throw new IllegalArgumentException(OTSErrorMessage.MULTI_VERSION_TIMESTAMP_IS_EMPTY);
            }
            
            if (value.getRawData() == null) {
                updateChange.deleteColumn(columnName, timestamp.asLong());
                return;
            }
            
            ColumnValue otsValue = ColumnConversion.columnToColumnValue(value);
            
            com.alicloud.openservices.tablestore.model.Column c = new com.alicloud.openservices.tablestore.model.Column(
                    columnName, 
                    otsValue,
                    timestamp.asLong()
                    );
            updateChange.put(c);
            return;
        } catch (IllegalArgumentException e) {
            LOG.warn("parseToColumn fail : {}", e.getMessage(), e);
            CollectorUtil.collect(r, e.getMessage());
            return;
        } catch (DataXException e) {
            LOG.warn("parseToColumn fail : {}", e.getMessage(), e);
            CollectorUtil.collect(r, e.getMessage());
            return;
        }
    }
    
    /**
     * 基于特殊模式处理Record
     * 当所有Record转换为Column失败时，方法会返回null
     * @param tableName
     * @param type
     * @param pkColumns
     * @param records
     * @return
     * @throws Exception
     */
    public static OTSLine parseMultiVersionRecordToOTSLine(
            String tableName, 
            OTSOpType type, 
            Map<TablePrimaryKeySchema, Integer> pkColumns,
            String attrColumnNamePrefixFilter,
            PrimaryKey pk,
            List<Record> records) throws OTSCriticalException {
        
        switch(type) {
            case UPDATE_ROW:
                RowUpdateChange updateChange = new RowUpdateChange(tableName, pk);
                for (Record r : records) {
                    appendCellToRowUpdateChange(pkColumns, attrColumnNamePrefixFilter, r, updateChange);
                }
                if (updateChange.getColumnsToUpdate().isEmpty()) {
                    return null;
                } else {
                    return new OTSLine(pk, records, updateChange);
                }
            default:
                LOG.error("Bug branch, can not support : {}(OTSOpType)", type);
                throw new OTSCriticalException(String.format(OTSErrorMessage.UNSUPPORT, type));
        }
    }
}
