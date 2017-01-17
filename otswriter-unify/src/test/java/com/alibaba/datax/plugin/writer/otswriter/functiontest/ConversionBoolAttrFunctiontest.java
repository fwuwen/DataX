package com.alibaba.datax.plugin.writer.otswriter.functiontest;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.otswriter.common.BaseTest;
import com.alibaba.datax.plugin.writer.otswriter.common.Conf;
import com.alibaba.datax.plugin.writer.otswriter.common.OTSHelper;
import com.alibaba.datax.plugin.writer.otswriter.common.OTSRowBuilder;
import com.alibaba.datax.plugin.writer.otswriter.common.Utils;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector.RecordAndMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSMode;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.TableMeta;

/**
 * 主要是测试各种类型转换为Bool的行为
 */
public class ConversionBoolAttrFunctiontest extends BaseTest{
    
    public static String tableName = "ots_writer_conversion_bool_attr_ft";
    
    private static SyncClientInterface ots = Utils.getOTSClient();
    private static TableMeta tableMeta = null;
    
    @BeforeClass
    public static void setBeforeClass() {}
    
    @AfterClass
    public static void setAfterClass() {
        ots.shutdown();
    }
    
    @Before
    public void setup() throws Exception {
        tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk_0", PrimaryKeyType.INTEGER);

        OTSHelper.createTableSafe(ots, tableMeta);
    }
    
    @After
    public void teardown() {}
    
    // 传入值是String，用户指定的是Bool, 期待转换正常，且值符合预期
    // true, TRUE, TruE, false, FALSE, False
    @Test
    public void testStringToBool() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();

        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new StringColumn("true"));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(-1))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBoolean(true), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(0));
            r.addColumn(new StringColumn("TRUE"));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBoolean(true), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(1));
            r.addColumn(new StringColumn("TRUE"));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(1))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBoolean(true), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(2));
            r.addColumn(new StringColumn("false"));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(2))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBoolean(false), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(3));
            r.addColumn(new StringColumn("FALSE"));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(3))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBoolean(false), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(4));
            r.addColumn(new StringColumn("False"));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(4))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBoolean(false), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.BOOLEAN), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots, conf, input, expect);
    }
    
    @Test
    public void testIllegalStringToBool() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(0));
            r.addColumn(new StringColumn("FFFFF"));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : STRING, src value: FFFFF, expect type: BOOLEAN .");
            rms.add(rm);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.BOOLEAN), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        test(ots, conf, input, null, rms, false);
    }
    
    // 传入值是Int，用户指定的是Bool, 期待转换正常，且值符合预期
    // -121, -1, 0, 1, 11
    @Test
    public void testIntToBool() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new LongColumn(-121));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(-1))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBoolean(true), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(0));
            r.addColumn(new LongColumn(-1));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBoolean(true), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(1));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(1))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBoolean(false), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(2));
            r.addColumn(new LongColumn(1));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(2))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBoolean(true), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(3));
            r.addColumn(new LongColumn(11));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(3))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBoolean(true), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.BOOLEAN), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots, conf, input, expect);
    }
    // 传入值是Double，用户指定的是Bool, 期待转换异常，异常信息符合预期
    // -1.0, 0.0, 1.0
    @Test
    public void testDoubleToBool() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new DoubleColumn(-1.0));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : DOUBLE, src value: -1.0, expect type: BOOLEAN .");
            rms.add(rm);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(0));
            r.addColumn(new DoubleColumn(0.0));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : DOUBLE, src value: 0.0, expect type: BOOLEAN .");
            rms.add(rm);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(1));
            r.addColumn(new DoubleColumn(1.0));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : DOUBLE, src value: 1.0, expect type: BOOLEAN .");
            rms.add(rm);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.BOOLEAN), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        test(ots, conf, input, null, rms, false);
    }
    // 传入值是Bool，用户指定的是Bool, 期待转换正常，且值符合预期
    // true, false
    @Test
    public void testBoolToBool() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new BoolColumn(true));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(-1))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBoolean(true), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(0));
            r.addColumn(new BoolColumn(false));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBoolean(false), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.BOOLEAN), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots, conf, input, expect);
    }
    // 传入值是Binary，用户指定的是Bool, 期待转换异常，异常信息符合预期
    @Test
    public void BinaryToBool() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new BytesColumn("try you best for succ".getBytes("UTF-8")));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : BYTES, src value: try you best for succ, expect type: BOOLEAN .");
            rms.add(rm);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.BOOLEAN), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        test(ots, conf, input, null, rms, false);
    }
}
