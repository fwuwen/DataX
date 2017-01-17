package com.alibaba.datax.plugin.writer.otswriter.unittest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import com.alibaba.datax.plugin.writer.otswriter.model.TablePrimaryKeySchema;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.otswriter.OTSCriticalException;
import com.alibaba.datax.plugin.writer.otswriter.common.DataChecker;
import com.alibaba.datax.plugin.writer.otswriter.common.OTSPrimaryKeyBuilder;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector.RecordAndMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSLine;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.utils.CollectorUtil;
import com.alibaba.datax.plugin.writer.otswriter.utils.ParseRecord;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange.Type;

/**
 * 测试目的：测试在各种模式组合下的解析行为
 * 
 * * 普通模式的处理
 * ** 正常
 * ** pk有部分列为空的情况
 * ** attr有部分列为空的情况
 * ** 指定ts的情况
 * ** 不指定ts的情况
 * ** pk列部分不能转换
 * ** attr列部分列不能转换
 * 
 * * 多版本模式的处理
 * ** 正常
 * ** columnName为空
 * ** columnName不能成功转换
 * ** ts为空
 * ** ts不能成功转换
 * ** value为空
 * ** value不能成功转换
 * 
 * @author redchen
 *
 */
public class ParseRecordUnittest {
    
    private TestPluginCollector collector = null;
    
    private void initCollector() {
        collector = new TestPluginCollector(Configuration.newDefault(), null, null);
        CollectorUtil.init(collector);
    }
    
    @Before
    public void setup() {
        initCollector();
    }
    
    /**
     * * 普通模式的处理
     * ** 正常
     * @throws OTSCriticalException 
     */
    @Test
    public void test_normal_valid_put() throws OTSCriticalException {
        // user configuration
        LinkedHashMap<TablePrimaryKeySchema, Integer> pkColumns = new LinkedHashMap<TablePrimaryKeySchema, Integer>();
        pkColumns.put(new TablePrimaryKeySchema("pk_0", PrimaryKeyType.STRING), 0);
        pkColumns.put(new TablePrimaryKeySchema("pk_1", PrimaryKeyType.INTEGER), 1);
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr_1", ColumnType.INTEGER));
        
        // input data
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new LongColumn(1));
        record.addColumn(new StringColumn("中文"));
        record.addColumn(new LongColumn(-1));
        
        OTSLine line = ParseRecord.parseNormalRecordToOTSLine("xx", OTSOpType.PUT_ROW, pkColumns, attrColumns, record, -1);
        
        // check
        
        {
            PrimaryKey pk = OTSPrimaryKeyBuilder.newInstance()
                    .add("pk_0", PrimaryKeyValue.fromString("hello"))
                    .add("pk_1", PrimaryKeyValue.fromLong(1))
                    .toPrimaryKey();
            
            assertEquals(pk, line.getPk());
        }
        
        {
            RowPutChange change = (RowPutChange) line.getRowChange();
            
            List<Column> columns = change.getColumnsToPut();
            
            assertEquals(2, columns.size());
            assertEquals("attr_0", columns.get(0).getName());
            assertEquals(ColumnType.STRING, columns.get(0).getValue().getType());
            assertEquals("中文", columns.get(0).getValue().asString());
            assertEquals("attr_1", columns.get(1).getName());
            assertEquals(ColumnType.INTEGER, columns.get(1).getValue().getType());
            assertEquals(-1, columns.get(1).getValue().asLong());
        }
        
        assertEquals(0, collector.getRecord().size());
    }
    
    /**
     * * 普通模式的处理
     * ** 正常
     * @throws OTSCriticalException 
     */
    @Test
    public void test_normal_valid_update() throws OTSCriticalException {
        // user configuration
        LinkedHashMap<TablePrimaryKeySchema, Integer> pkColumns = new LinkedHashMap<TablePrimaryKeySchema, Integer>();
        pkColumns.put(new TablePrimaryKeySchema("pk_0", PrimaryKeyType.STRING), 0);
        pkColumns.put(new TablePrimaryKeySchema("pk_1", PrimaryKeyType.INTEGER), 1);
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr_1", ColumnType.INTEGER));
        
        // input data
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new LongColumn(1));
        record.addColumn(new StringColumn("中文"));
        record.addColumn(new LongColumn(-1));
        
        OTSLine line = ParseRecord.parseNormalRecordToOTSLine("xx", OTSOpType.UPDATE_ROW, pkColumns, attrColumns, record, -1);
        
        // check
        
        {
            PrimaryKey pk = OTSPrimaryKeyBuilder.newInstance()
                    .add("pk_0", PrimaryKeyValue.fromString("hello"))
                    .add("pk_1", PrimaryKeyValue.fromLong(1))
                    .toPrimaryKey();
            
            assertEquals(pk, line.getPk());
        }
        
        {
            RowUpdateChange change = (RowUpdateChange) line.getRowChange();
            
            List<Pair<Column, Type>> columns = change.getColumnsToUpdate();
            
            assertEquals(2, columns.size());
            assertEquals(Type.PUT, columns.get(0).getSecond());
            assertEquals("attr_0", columns.get(0).getFirst().getName());
            assertEquals(ColumnType.STRING, columns.get(0).getFirst().getValue().getType());
            assertEquals("中文", columns.get(0).getFirst().getValue().asString());
            
            assertEquals(Type.PUT, columns.get(1).getSecond());
            assertEquals("attr_1", columns.get(1).getFirst().getName());
            assertEquals(ColumnType.INTEGER, columns.get(1).getFirst().getValue().getType());
            assertEquals(-1, columns.get(1).getFirst().getValue().asLong());
        }
        
        assertEquals(0, collector.getRecord().size());
    }
    
    /**
     * * 普通模式的处理
     * ** pk有部分列为空的情况
     * @throws OTSCriticalException 
     */
    @Test
    public void test_normal_pk_null_column() throws OTSCriticalException {
        // user configuration
        LinkedHashMap<TablePrimaryKeySchema, Integer> pkColumns = new LinkedHashMap<TablePrimaryKeySchema, Integer>();
        pkColumns.put(new TablePrimaryKeySchema("pk_0", PrimaryKeyType.STRING), 0);
        pkColumns.put(new TablePrimaryKeySchema("pk_1", PrimaryKeyType.INTEGER), 1);
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr_1", ColumnType.INTEGER));
        
        // input
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new LongColumn());
        record.addColumn(new StringColumn("中文"));
        record.addColumn(new LongColumn(-1));
        
        OTSLine line = ParseRecord.parseNormalRecordToOTSLine("xx", OTSOpType.PUT_ROW, pkColumns, attrColumns, record, -1);
        assertEquals(null, line);
        
        assertEquals(1, collector.getRecord().size());
        
        List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
        expect.add(new RecordAndMessage(record, "The column of record is NULL, primary key name : pk_1 ."));
        
        assertTrue(DataChecker.checkRecordWithMessage(collector.getContent(), expect));
    }
    
    /**
     * * 普通模式的处理
     * ** attr有部分列为空的情况
     * @throws OTSCriticalException 
     */
    @Test
    public void test_normal_attr_null_column_put() throws OTSCriticalException {
        // user configuration
        LinkedHashMap<TablePrimaryKeySchema, Integer> pkColumns = new LinkedHashMap<TablePrimaryKeySchema, Integer>();
        pkColumns.put(new TablePrimaryKeySchema("pk_0", PrimaryKeyType.STRING), 0);
        pkColumns.put(new TablePrimaryKeySchema("pk_1", PrimaryKeyType.INTEGER), 1);
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr_1", ColumnType.INTEGER));
        
        // input
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new LongColumn(1));
        record.addColumn(new StringColumn());
        record.addColumn(new LongColumn(-1));
        
        OTSLine line = ParseRecord.parseNormalRecordToOTSLine("xx", OTSOpType.PUT_ROW, pkColumns, attrColumns, record, -1);
        
        // check
        
        {
            PrimaryKey pk = OTSPrimaryKeyBuilder.newInstance()
                    .add("pk_0", PrimaryKeyValue.fromString("hello"))
                    .add("pk_1", PrimaryKeyValue.fromLong(1))
                    .toPrimaryKey();
            
            assertEquals(pk, line.getPk());
        }
        
        {
            RowPutChange change = (RowPutChange) line.getRowChange();
            
            List<Column> columns = change.getColumnsToPut();
            
            assertEquals(1, columns.size());
            assertEquals("attr_1", columns.get(0).getName());
            assertEquals(ColumnType.INTEGER, columns.get(0).getValue().getType());
            assertEquals(-1, columns.get(0).getValue().asLong());
        }
        
        assertEquals(0, collector.getRecord().size());
    }
    
    /**
     * * 普通模式的处理
     * ** attr有部分列为空的情况
     * @throws OTSCriticalException 
     */
    @Test
    public void test_normal_attr_null_column_update() throws OTSCriticalException {
        // user configuration
        LinkedHashMap<TablePrimaryKeySchema, Integer> pkColumns = new LinkedHashMap<TablePrimaryKeySchema, Integer>();
        pkColumns.put(new TablePrimaryKeySchema("pk_0", PrimaryKeyType.STRING), 0);
        pkColumns.put(new TablePrimaryKeySchema("pk_1", PrimaryKeyType.INTEGER), 1);
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr_1", ColumnType.INTEGER));
        
        // input
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new LongColumn(1));
        record.addColumn(new StringColumn());
        record.addColumn(new LongColumn(-1));
        
        OTSLine line = ParseRecord.parseNormalRecordToOTSLine("xx", OTSOpType.UPDATE_ROW, pkColumns, attrColumns, record, -1);
        
        // check
        
        {
            PrimaryKey pk = OTSPrimaryKeyBuilder.newInstance()
                    .add("pk_0", PrimaryKeyValue.fromString("hello"))
                    .add("pk_1", PrimaryKeyValue.fromLong(1))
                    .toPrimaryKey();
            
            assertEquals(pk, line.getPk());
        }
        
        {
            RowUpdateChange change = (RowUpdateChange) line.getRowChange();
            
            List<Pair<Column, Type>> columns = change.getColumnsToUpdate();
            
            assertEquals(2, columns.size());
            
            assertEquals(Type.DELETE_ALL, columns.get(0).getSecond());
            assertEquals("attr_0", columns.get(0).getFirst().getName());
            assertEquals(ColumnType.STRING, columns.get(0).getFirst().getValue().getType());
            assertEquals(null, columns.get(0).getFirst().getValue().asString());
            
            assertEquals(Type.PUT, columns.get(1).getSecond());
            assertEquals("attr_1", columns.get(1).getFirst().getName());
            assertEquals(ColumnType.INTEGER, columns.get(1).getFirst().getValue().getType());
            assertEquals(-1, columns.get(1).getFirst().getValue().asLong());
        }
        
        assertEquals(0, collector.getRecord().size());
    }
    
    /**
     * * 普通模式的处理
     * ** 指定ts的情况
     * @throws OTSCriticalException 
     */
    @Test
    public void test_normal_specify_ts() throws OTSCriticalException {
        // user configuration
        LinkedHashMap<TablePrimaryKeySchema, Integer> pkColumns = new LinkedHashMap<TablePrimaryKeySchema, Integer>();
        pkColumns.put(new TablePrimaryKeySchema("pk_0", PrimaryKeyType.STRING), 0);
        pkColumns.put(new TablePrimaryKeySchema("pk_1", PrimaryKeyType.INTEGER), 1);
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr_1", ColumnType.INTEGER));
        
        // input
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new LongColumn(1));
        record.addColumn(new StringColumn());
        record.addColumn(new LongColumn(-1));
        
        long ts = System.currentTimeMillis();
        
        OTSLine line = ParseRecord.parseNormalRecordToOTSLine("xx", OTSOpType.PUT_ROW, pkColumns, attrColumns, record, ts);
        
        // check
        
        {
            PrimaryKey pk = OTSPrimaryKeyBuilder.newInstance()
                    .add("pk_0", PrimaryKeyValue.fromString("hello"))
                    .add("pk_1", PrimaryKeyValue.fromLong(1))
                    .toPrimaryKey();
            
            assertEquals(pk, line.getPk());
        }
        
        {
            RowPutChange change = (RowPutChange) line.getRowChange();
            
            List<Column> columns = change.getColumnsToPut();
            
            assertEquals(1, columns.size());
            assertEquals("attr_1", columns.get(0).getName());
            assertEquals(ColumnType.INTEGER, columns.get(0).getValue().getType());
            assertEquals(-1, columns.get(0).getValue().asLong());
            assertEquals(ts, columns.get(0).getTimestamp());
        }
        
        assertEquals(0, collector.getRecord().size());
    }
    
    /**
     * * 普通模式的处理
     * ** pk有部分列转换失败
     * @throws OTSCriticalException 
     */
    @Test
    public void test_normal_pk_column_conversion_fail() throws OTSCriticalException {
        // user configuration
        LinkedHashMap<TablePrimaryKeySchema, Integer> pkColumns = new LinkedHashMap<TablePrimaryKeySchema, Integer>();
        pkColumns.put(new TablePrimaryKeySchema("pk_0", PrimaryKeyType.STRING), 0);
        pkColumns.put(new TablePrimaryKeySchema("pk_1", PrimaryKeyType.INTEGER), 1);
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr_1", ColumnType.INTEGER));
        
        // input
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new StringColumn("中文"));
        record.addColumn(new LongColumn(-1));
        
        OTSLine line = ParseRecord.parseNormalRecordToOTSLine("xx", OTSOpType.PUT_ROW, pkColumns, attrColumns, record, -1);
        assertEquals(null, line);
        
        assertEquals(1, collector.getRecord().size());
        
        List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
        expect.add(new RecordAndMessage(record, "Column coversion error, src type : STRING, src value: hello, expect type: INTEGER ."));
        
        assertTrue(DataChecker.checkRecordWithMessage(collector.getContent(), expect));
    }
    
    /**
     * * 普通模式的处理
     * ** attr有部分列转换失败
     * @throws OTSCriticalException 
     */
    @Test
    public void test_normal_attr_column_conversion_fail() throws OTSCriticalException {
        // user configuration
        LinkedHashMap<TablePrimaryKeySchema, Integer> pkColumns = new LinkedHashMap<TablePrimaryKeySchema, Integer>();
        pkColumns.put(new TablePrimaryKeySchema("pk_0", PrimaryKeyType.STRING), 0);
        pkColumns.put(new TablePrimaryKeySchema("pk_1", PrimaryKeyType.INTEGER), 1);
        
        List<OTSAttrColumn> attrColumns = new ArrayList<OTSAttrColumn>();
        attrColumns.add(new OTSAttrColumn("attr_0", ColumnType.STRING));
        attrColumns.add(new OTSAttrColumn("attr_1", ColumnType.BINARY));
        
        // input
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new LongColumn(1));
        record.addColumn(new StringColumn("中文"));
        record.addColumn(new LongColumn(-1));
        
        OTSLine line = ParseRecord.parseNormalRecordToOTSLine("xx", OTSOpType.UPDATE_ROW, pkColumns, attrColumns, record, -1);
        assertEquals(null, line);
        
        assertEquals(1, collector.getRecord().size());
        
        List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
        expect.add(new RecordAndMessage(record, "Column coversion error, src type : LONG, src value: -1, expect type: BINARY ."));
        
        assertTrue(DataChecker.checkRecordWithMessage(collector.getContent(), expect));
    }
}
