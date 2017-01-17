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
 * 主要是测试各种类型转换为Binary的行为
 */
public class ConversionBinaryAttrFunctiontest extends BaseTest{
    
    public static String tableName = "ots_writer_conversion_binary_attr_ft";
    
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
    
    // 传入值是String，用户指定的是Binary, 期待转换正常，且值符合预期
    @Test
    public void testStringToBinary() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        
        String ss = "在0.12.1版本及其以上的chunkserver 热升级中，chunkserver的suicide不会等到数据落盘退出，在使用mem log file场景下会造成数据丢失，该bug已经ci，但飞天sdk还未发出 请SLS也关注此bug。";
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new StringColumn(ss));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(-1))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBinary(ss.getBytes("UTF-8")), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.BINARY), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots,conf, rs, expect);
    }
    // 传入值是Int，用户指定的是Binary, 期待转换异常，异常信息符合预期
    @Test
    public void testIntToBinary() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new LongColumn(999));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : LONG, src value: 999, expect type: BINARY .");
            rms.add(rm);
        }
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.BINARY), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        test(ots, conf, input, null, rms, false);
    }
    // 传入值是Double，用户指定的是Binary, 期待转换异常，异常信息符合预期
    @Test
    public void testDoubleToBinary() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new DoubleColumn(111.0));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : DOUBLE, src value: 111.0, expect type: BINARY .");
            rms.add(rm);
        }
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.BINARY), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        test(ots, conf, input, null, rms, false);
    }
    // 传入值是Bool，用户指定的是Binary, 期待转换异常，异常信息符合预期
    @Test
    public void testBoolToBinary() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new BoolColumn(true));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : BOOL, src value: true, expect type: BINARY .");
            rms.add(rm);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new BoolColumn(false));
            input.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "Column coversion error, src type : BOOL, src value: false, expect type: BINARY .");
            rms.add(rm);
        }
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.BINARY), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        test(ots, conf, input, null, rms, false);
    }
    // 传入值是Binary，用户指定的是Binary, 期待转换正常，且值符合预期
    @Test
    public void testBinaryToBinary() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        
        byte [] ss = "we4=-t-=gtatrgtaewgrt".getBytes("UTF-8");
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(-1));
            r.addColumn(new BytesColumn(ss));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromLong(-1))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBinary(ss), 1)
                    .toRow();

            expect.add(row);
        }
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(1, ColumnType.BINARY), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots,conf, rs, expect);
    }
}
