package com.alibaba.datax.plugin.writer.otswriter.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.Column.Type;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector.RecordAndMessage;
import com.alicloud.openservices.tablestore.model.*;

public class DataChecker {
    
    private static final Logger LOG = LoggerFactory.getLogger(DataChecker.class);
    
    private static String columnValueToString(ColumnValue value) {
        switch (value.getType()) {
            case BINARY:
                return "BINARY:";
            case BOOLEAN:
                return "BOOLEAN:" + value.asBoolean();
            case DOUBLE:
                return "DOUBLE:" + value.asDouble();
            case INTEGER:
                return "INTEGER:" + value.asLong();
            case STRING:
                return "STRING:" + value.asString();
            default:
                return null;
        }
    }
    
    private static boolean cmpRowWithNoTS(Row src, Row expect) {
        com.alicloud.openservices.tablestore.model.Column[] srcColumn = src.getColumns();
        com.alicloud.openservices.tablestore.model.Column[] expectColumn = expect.getColumns();
        // 判断个数是否相等
        if (srcColumn.length != expectColumn.length) {
            LOG.error(
                    "ComWithNoTS, Row:(PK:({})), Expect row size not equal size in src, expect size : {}, size in src : {}", 
                    new Object[] {expect.getPrimaryKey().toString(),  expectColumn.length, srcColumn.length}
                    );
            return false;
        }
        // 
        for (int i = 0; i < srcColumn.length; i++) {
            com.alicloud.openservices.tablestore.model.Column sc = srcColumn[i];
            com.alicloud.openservices.tablestore.model.Column ec = expectColumn[i];
            
            if (!(sc.getName().equals(ec.getName()) && sc.getValue().equals(ec.getValue()))) {
                LOG.error(
                        "ComWithNoTS, Row:(PK:({})), Expect cell not equal in src, expect cell : {}, cell in src : {}", 
                        new Object[] {expect.getPrimaryKey().toString(),  ec.toString(), sc.toString()}
                        );
                return false;
            }
        }
        return true;
    }
    
    private static boolean cmpRowWithTS(Row src, Row expect) {
        if (src.getColumns().length != expect.getColumns().length) {
            LOG.error(
                    "CmpRowWithTS, Row:(PK:({})), Expect row size not equal size in src, expect size : {}, size in src : {}", 
                    new Object[] {expect.getPrimaryKey().toString(),  expect.getColumns().length, src.getColumns().length}
                    );
            return false;
        }

        for (Entry<String, NavigableMap<Long, ColumnValue>> c : expect.getColumnsMap().entrySet()) {
            NavigableMap<Long, ColumnValue> cells = src.getColumnsMap().get(c.getKey());
            if (cells == null) {
                LOG.error("Can not get column ({}) from row in src.", c.getKey());
                return false;
            }
            if (c.getValue().size() != cells.size()) {
                LOG.error(
                        "CmpRowWithTS, Row:(PK:({}), ColumnName:{}), the version count not equal, expect size:{}, size in src: {}.",
                        new Object[] {expect.getPrimaryKey().toString(), c.getKey(), c.getValue().size(), cells.size()}
                        );
            }
            
            for (Entry<Long, ColumnValue> s : c.getValue().entrySet()) {
                ColumnValue value = cells.get(s.getKey());
                if (value == null) {
                    LOG.error(
                            "CmpRowWithTS, Row:(PK:({}), ColumnName:{}), can not get the ts({}) from src.", 
                            new Object [] {expect.getPrimaryKey().toString(), c.getKey(), s.getKey()}
                            );
                    return false;
                }
                if (!value.equals(s.getValue())) {
                    LOG.error(
                            "Row:(PK:({}), ColumnName:{}, TS:{}), the expect column value({}) not equal column value({}) in src.", 
                            new Object [] {expect.getPrimaryKey().toString(), c.getKey(), s.getKey(), columnValueToString(s.getValue()), columnValueToString(value)}
                            );
                    return false;
                }
            }
        }
        return true;
    }
    
    private static Map<PrimaryKey, Row> buildMapping(List<Row> src) {
        Map<PrimaryKey, Row> srcMapping = new HashMap<PrimaryKey, Row>();
        for (Row r : src) {
            srcMapping.put(r.getPrimaryKey(), r);
        }
        return srcMapping;
    }
    
    private static int compareBytes(byte[] b1, byte[] b2) {
        int size = b1.length < b2.length ? b1.length : b2.length;
        for (int i = 0; i < size; i++) {
            int r = b1[i] - b2[i];
            if (r != 0) {
                return r;
            }
        }
        if (b1.length > b2.length) {
            return 1;
        } else if (b1.length < b2.length){
            return -1;
        } else {
            return 0;
        }
    }
    
    private static boolean cmpRecord(Record src, Record target) {
        if (src.getColumnNumber() != target.getColumnNumber()) {
            LOG.error("src size({}) not equal target size({}).", src.getColumnNumber() , target.getColumnNumber());
            return false;
        }
        for (int i = 0; i < src.getColumnNumber(); i++) {
            Column srcValue = src.getColumn(i);
            Column targetValue = target.getColumn(i);
            
            if (srcValue.getType() == Type.BYTES && targetValue.getType() == Type.BYTES) {
                if (srcValue.getRawData() == null || targetValue.getRawData() == null) {
                    if (!(srcValue.getRawData() == null && targetValue.getRawData() == null)) {
                        LOG.error("targetValue({}) not equal srcValue({}), . index({}) in record.", new Object[] {targetValue.asDouble(), srcValue.asDouble(), i});
                        return false;
                    }
                } else {
                    if (compareBytes(srcValue.asBytes(), targetValue.asBytes()) != 0) {
                        LOG.error("Binary not equal.");
                        return false;
                    }
                }
            } else {
                if (srcValue.getType() != targetValue.getType()) {
                    LOG.error("targetValue type({}) not equal srcValue type({}), . index({}) in record.", new Object[] {targetValue.getType(), srcValue.getType(), i});
                    return false;
                } else {
                    if (srcValue.getRawData() == null || targetValue.getRawData() == null) {
                        if (!(srcValue.getRawData() == null && targetValue.getRawData() == null)) {
                            LOG.error("targetValue({}) not equal srcValue({}), . index({}) in record.", new Object[] {targetValue.asDouble(), srcValue.asDouble(), i});
                            return false;
                        }
                    } else {
                        switch (srcValue.getType()) {
                            case BOOL:
                                if (srcValue.asBoolean() == targetValue.asBoolean()) {
                                    break;
                                }
                                if (srcValue.asBoolean().booleanValue() != targetValue.asBoolean().booleanValue()) {
                                    LOG.error("targetValue({}) not equal srcValue({}), . index({}) in record.", new Object[] {targetValue.asBoolean(), srcValue.asBoolean(), i});
                                    return false;
                                }
                                break;
                            case DOUBLE:
                                if (srcValue.asDouble() == targetValue.asDouble()) {
                                    break;
                                }
                                if (srcValue.asDouble().doubleValue() != targetValue.asDouble().doubleValue()) {
                                    LOG.error("targetValue({}) not equal srcValue({}), . index({}) in record.", new Object[] {targetValue.asDouble(), srcValue.asDouble(), i});
                                    return false;
                                }
                                break;
                            case LONG:
                                if (srcValue.asLong() == targetValue.asLong()) {
                                    break;
                                }
                                if (srcValue.asLong().longValue() != targetValue.asLong().longValue()) {
                                    LOG.error("targetValue({}) not equal srcValue({}), . index({}) in record.", new Object[] {targetValue.asLong(), srcValue.asLong(), i});
                                    return false;
                                }
                                break;
                            case STRING:
                                if (srcValue.asString() == targetValue.asString()) {
                                    break;
                                }
                                if (!srcValue.asString().equals(targetValue.asString())) {
                                    LOG.error("targetValue({}) not equal srcValue({}), . index({}) in record.", new Object[] {targetValue.asString(), srcValue.asString(), i});
                                    return false;
                                }
                                break;
                            default:
                                break;
                                
                        }
                    }
                }
            }
        }
        return true;
    }
    
    public static boolean checkRow(List<Row> src, List<Row> expect, boolean isCheckTS) {
        for (Row r : src) {
            System.out.println(r.toString());
        }
        if (src.size() != expect.size()) {
            LOG.error("Expect size not equal size in src, expect size : {}, size in src : {}", expect.size(), src.size());
            return false;
        }
        
        // build mapping
        Map<PrimaryKey, Row> srcMapping = buildMapping(src);
        
        for (Row r : expect) {
            Row s = srcMapping.get(r.getPrimaryKey());
            if (s == null) {
                LOG.error("Can not get row (PK:({})) from src.", r.getPrimaryKey().toString());
                return false;
            }
            
            if (isCheckTS) {
                if (!cmpRowWithTS(s, r)){
                    return false;
                }
            } else {
                if (!cmpRowWithNoTS(s, r)){
                    return false;
                }
            }
        }
        return true;
    }
    
    public static boolean checkRowsCountPerRequest(List<Integer> src, List<Integer> expect) {
        if (expect.size() != src.size()) {
            LOG.error("Expect size not equal size in ots, expect size : {}, size in ots : {}", expect.size(), src.size());
            return false;
        }
        
        for (int i = 0; i < expect.size(); i++) {
            if (expect.get(i) != src.get(i)) {
                LOG.error("Expect({}) not equal src({}), index : {}", new Object[]{expect.get(i), src.get(i), i});
                return false;
            }
        }
        return true;
    }
    
    public static boolean checkRecord(List<Record> src, List<Record> expect) {
        if (src.size() != expect.size()) {
            LOG.error("Expect size({}) not equal size({}) of src.", expect.size(), src.size());
            return false;
        }
        int size = src.size();
        for (int i = 0; i < size; i++) {
            Record expectRecord = expect.get(i);
            Record srcRecord = src.get(i);
            
            if (expectRecord.getColumnNumber() != srcRecord.getColumnNumber()) {
                LOG.error("Expect number({}) not equal number({}) of src, Index : {}", new Object[]{expectRecord.getColumnNumber(), srcRecord.getColumnNumber(), i});
                return false;
            }
            
            int number = expectRecord.getColumnNumber();
            for (int j = 0; j < number; j++) {
                if (!cmpRecord(srcRecord, expectRecord)) {
                    return false;
                }
            }
        }
        return true;
    }
    
    public static boolean checkRecordWithMessage(List<RecordAndMessage> src, List<RecordAndMessage> expect) {
        if (src.size() != expect.size()) {
            LOG.error("Expect size({}) not equal size({}) of src.", expect.size(), src.size());
            return false;
        }
        int size = src.size();
        for (int i = 0; i < size; i++) {
            Record expectRecord = expect.get(i).getDirtyRecord();
            Record srcRecord = src.get(i).getDirtyRecord();
            
            if (expectRecord.getColumnNumber() != srcRecord.getColumnNumber()) {
                LOG.error("Expect number({}) not equal number({}) of src, Index : {}", new Object[]{expectRecord.getColumnNumber(), srcRecord.getColumnNumber(), i});
                return false;
            }
            
            int number = expectRecord.getColumnNumber();
            for (int j = 0; j < number; j++) {
                if (!cmpRecord(srcRecord, expectRecord)) {
                    return false;
                }
            }
            
            String expectMsg = expect.get(i).getErrorMessage();
            String srcMsg = src.get(i).getErrorMessage();
            
            if (!expectMsg.equals(srcMsg)) {
                LOG.error("Expect Message({}) not equal Message({}) of src, Index : {}", new Object[]{expectMsg, srcMsg, i});
                return false;
            }
        }
        return true;
    }
}
