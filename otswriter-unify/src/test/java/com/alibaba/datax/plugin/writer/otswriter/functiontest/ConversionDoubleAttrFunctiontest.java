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
 * 主要是测试各种类型转换为Double的行为
 */
public class ConversionDoubleAttrFunctiontest extends BaseTest{
    
    public static String tableName = "ots_writer_conversion_double_attr_ft";
    
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
        tableMeta.addPrimaryKeyColumn("pk_0", PrimaryKeyType.STRING);

        OTSHelper.createTableSafe(ots, tableMeta);
    }
    
    @After
    public void teardown() {}
    
    // 传入值是String，用户指定的是Double, 期待转换正常，且值符合预期
    // -100, -23.1, 0, 43.01, 111
    @Test
    public void testStringToDouble() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        
        // 传入值是String
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("-100"));
            r.addColumn(new StringColumn("-100"));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("-100"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromDouble(-100.0), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("-23.1"));
            r.addColumn(new StringColumn("-23.1"));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("-23.1"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromDouble(-23.1), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new StringColumn("0"));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromDouble(0.0), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("43.01"));
            r.addColumn(new StringColumn("43.01"));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("43.01"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromDouble(43.01), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("111"));
            r.addColumn(new StringColumn("111"));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("111"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromDouble(111.0), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.DOUBLE), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots, conf, input, expect);
    }
    // 传入值是String，但是是非数值型的字符串，如“world”， “100L”， “0xff”，用户指定的是Double, 期待转换异常，异常信息符合预期
    @Test
    public void testIllegalStringToDouble() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("world"));
            r.addColumn(new StringColumn("world"));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : STRING, src value: world, expect type: DOUBLE .");
            rms.add(rm);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("100L"));
            r.addColumn(new StringColumn("100L"));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : STRING, src value: 100L, expect type: DOUBLE .");
            rms.add(rm);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0xff"));
            r.addColumn(new StringColumn("0xff"));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : STRING, src value: 0xff, expect type: DOUBLE .");
            rms.add(rm);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.DOUBLE), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        test(ots, conf, input, null, rms, false);
    }
    // 传入值是Int，用户指定的是Double, 期待转换正常，且值符合预期
    // Long.min, Long.max, 0
    @Test
    public void testIntToDouble() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_1"));
            r.addColumn(new LongColumn(Long.MIN_VALUE));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("row_1"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromDouble(Long.MIN_VALUE), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_2"));
            r.addColumn(new LongColumn(Long.MAX_VALUE));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("row_2"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromDouble(Long.MAX_VALUE), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_3"));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("row_3"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromDouble(0), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.DOUBLE), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots, conf, input, expect);
    }
    // 传入值是Double，用户指定的是Double, 期待转换正常，且值符合预期
    // Double.min, Double.max, 0
    @Test
    public void testDoubleToDouble() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_1"));
            r.addColumn(new DoubleColumn(Double.MIN_VALUE));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("row_1"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromDouble(Double.MIN_VALUE), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_2"));
            r.addColumn(new DoubleColumn(Double.MAX_VALUE));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("row_2"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromDouble(Double.MAX_VALUE), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_3"));
            r.addColumn(new DoubleColumn(0));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("row_3"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromDouble(0), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.DOUBLE), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots, conf, input, expect);
    }
    // 传入值是Bool，用户指定的是Double, 期待转换正常，且值符合预期
    // true, false
    @Test
    public void testBoolToDouble() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_1"));
            r.addColumn(new BoolColumn(true));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("row_1"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromDouble(1.0), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_2"));
            r.addColumn(new BoolColumn(false));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("row_2"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromDouble(0.0), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.DOUBLE), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots, conf, input, expect);
    }
    // 传入值是Binary，用户指定的是Double, 期待转换异常，异常信息符合预期
    @Test
    public void testBinaryToDouble() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("row_1"));
            r.addColumn(new BytesColumn("最低指数归一化双变量。它等于Math.getExponent返回的值".getBytes("UTF-8")));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : BYTES, src value: 最低指数归一化双变量。它等于Math.getExponent返回的值, expect type: DOUBLE .");
            rms.add(rm);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.DOUBLE), 
                OTSOpType.PUT_ROW,
                OTSMode.NORMAL);
        test(ots, conf, input, null, rms, false);
    }
}
