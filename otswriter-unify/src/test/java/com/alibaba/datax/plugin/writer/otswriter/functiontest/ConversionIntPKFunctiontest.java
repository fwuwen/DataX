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
 * 主要是测试各种类型转换为Int的行为
 */
public class ConversionIntPKFunctiontest extends BaseTest{
    
    public static String tableName = "ots_writer_conversion_int_pk_ft";
    
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
        tableMeta.addPrimaryKeyColumn("pk_1", PrimaryKeyType.INTEGER);

        OTSHelper.createTableSafe(ots, tableMeta);
    }
    
    @After
    public void teardown() {}
    

    
    /**
     * 传入 : 值是String，用户指定的是Int
     * 期待 : 转换正常，且值符合预期
     * @throws Exception
     */
    @Test
    public void testStringToInt() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        
        // 传入值是String
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("-100"));
            r.addColumn(new StringColumn("-100"));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(-100))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(-100))
                    .addAttrColumn(getColumnName(0),  ColumnValue.fromLong(0), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("10E2"));
            r.addColumn(new StringColumn("10E2"));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(1000))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(1000))
                    .addAttrColumn(getColumnName(0),  ColumnValue.fromLong(0), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new StringColumn("0"));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(0))
                    .addAttrColumn(getColumnName(0),  ColumnValue.fromLong(0), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots, conf, input, expect);
    }
    
    /**
     * 传入值是Int，用户指定的是Int, 期待转换正常，且值符合预期
     * @throws Exception
     */
    @Test
    public void testIntToInt() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        
        // 传入值是Int
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(Long.MIN_VALUE));
            r.addColumn(new LongColumn(Long.MIN_VALUE));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(Long.MIN_VALUE))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(Long.MIN_VALUE))
                    .addAttrColumn(getColumnName(0),  ColumnValue.fromLong(0), 1)
                    .toRow();
            expect.add(row);
            
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(Long.MAX_VALUE - 1));
            r.addColumn(new LongColumn(Long.MAX_VALUE - 1));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(Long.MAX_VALUE - 1))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(Long.MAX_VALUE - 1))
                    .addAttrColumn(getColumnName(0),  ColumnValue.fromLong(0), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(0));
            r.addColumn(new LongColumn(0));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(0))
                    .addAttrColumn(getColumnName(0),  ColumnValue.fromLong(0), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots, conf, input, expect);
    }
    
    /**
     * 传入值是Double，用户指定的是Int, 期待转换正常，且值符合预期
     * @throws Exception
     */
    @Test
    public void testDoubleToInt() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        
        // 传入值是Double
        {
            Record r = new DefaultRecord();
            r.addColumn(new DoubleColumn(-9012.023));
            r.addColumn(new DoubleColumn(-9012.023));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(-9012))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(-9012))
                    .addAttrColumn(getColumnName(0),  ColumnValue.fromLong(0), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new DoubleColumn(0));
            r.addColumn(new DoubleColumn(0));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(0))
                    .addAttrColumn(getColumnName(0),  ColumnValue.fromLong(0), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new DoubleColumn(1211.12));
            r.addColumn(new DoubleColumn(1211.12));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(1211))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(1211))
                    .addAttrColumn(getColumnName(0),  ColumnValue.fromLong(0), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots, conf, input, expect);
    }
    
    /**
     *  传入值是Bool，用户指定的是String, 期待转换正常，且值符合预期
     * @throws Exception
     */
    @Test
    public void testBoolToInt() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new BoolColumn(true));
            r.addColumn(new BoolColumn(true));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(1))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(1))
                    .addAttrColumn(getColumnName(0),  ColumnValue.fromLong(0), 1)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new BoolColumn(false));
            r.addColumn(new BoolColumn(false));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(0))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromLong(0))
                    .addAttrColumn(getColumnName(0),  ColumnValue.fromLong(0), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots, conf, input, expect);
    }

    /**
     * 用户输入Int，但是系统传入非数值型的字符串，期望写入OTS失败
     * 例如："", "hello", "0x5f", "100L"
     * @throws Exception
     */
    @Test
    public void testIllegalStringToInt() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(""));
            r.addColumn(new StringColumn(""));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : STRING, src value: , expect type: INTEGER .");
            rms.add(rm);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello"));
            r.addColumn(new StringColumn("hello"));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : STRING, src value: hello, expect type: INTEGER .");
            rms.add(rm);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0x5f"));
            r.addColumn(new StringColumn("0x5f"));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : STRING, src value: 0x5f, expect type: INTEGER .");
            rms.add(rm);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("100L"));
            r.addColumn(new StringColumn("100L"));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : STRING, src value: 100L, expect type: INTEGER .");
            rms.add(rm);
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        test(ots, conf, input, null, rms, false);
    }
    
    /**
     * 传入Binary，期望写入OTS失败
     * @throws Exception
     */
    @Test
    public void testBinaryToInt() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new BytesColumn("！@#￥%……&*（）——+“：{}？》《".getBytes("UTF-8")));
            r.addColumn(new BytesColumn("！@#￥%……&*（）——+“：{}？》《".getBytes("UTF-8")));
            r.addColumn(new LongColumn(0));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : BYTES, src value: ！@#￥%……&*（）——+“：{}？》《, expect type: INTEGER .");
            rms.add(rm);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        test(ots, conf, input, null, rms, false);
    }
}
