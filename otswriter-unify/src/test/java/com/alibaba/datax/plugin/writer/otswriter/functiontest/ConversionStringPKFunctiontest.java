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
 * 主要是测试各种类型转换为String的行为
 */
public class ConversionStringPKFunctiontest extends BaseTest{
   
    public static String tableName = "ConversionStringPKFunctiontest";
    
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
        tableMeta.addPrimaryKeyColumn("pk_1", PrimaryKeyType.STRING);

        OTSHelper.createTableSafe(ots, tableMeta);
    }
    
    @After
    public void teardown() {}
    
    /**
     * 传入 : 值是String，用户指定的是String 
     * 期待 : 转换正常，且值符合预期
     * @throws Exception
     */
    @Test
    public void testStringToString() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        long ts = 1;
        
        // 传入值是String
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(""));
            r.addColumn(new StringColumn(""));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(""))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString(""))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromLong(0), ts++)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("-100"));
            r.addColumn(new StringColumn("-100"));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("-100"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("-100"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromLong(0), ts++)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("100L"));
            r.addColumn(new StringColumn("100L"));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("100L"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("100L"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromLong(0), ts++)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0x5f"));
            r.addColumn(new StringColumn("0x5f"));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0x5f"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("0x5f"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromLong(0), ts++)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new StringColumn("0"));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("0"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromLong(0), ts++)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("(*^__^*) 嘻嘻……"));
            r.addColumn(new StringColumn("(*^__^*) 嘻嘻……"));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("(*^__^*) 嘻嘻……"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("(*^__^*) 嘻嘻……"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromLong(0), ts++)
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
        testWithNoTS(ots,conf, rs, expect);
    }

    // 传入值是Int，用户指定的是String, 期待转换正常，且值符合预期
    @Test
    public void testIntToString() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        long ts = 1;
        // 传入值是Int
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(Long.MIN_VALUE));
            r.addColumn(new LongColumn(Long.MIN_VALUE));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(String.valueOf(Long.MIN_VALUE)))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString(String.valueOf(Long.MIN_VALUE)))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromLong(0), ts++)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(Long.MAX_VALUE));
            r.addColumn(new LongColumn(Long.MAX_VALUE));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(String.valueOf(Long.MAX_VALUE)))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString(String.valueOf(Long.MAX_VALUE)))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromLong(0), ts++)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new LongColumn(0));
            r.addColumn(new LongColumn(0));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("0"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromLong(0), ts++)
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
        testWithNoTS(ots,conf, rs, expect);
    }
    
    // 传入值是Double，用户指定的是String, 期待转换正常，且值符合预期
    @Test
    public void testDoubleToString() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        long ts = 1;
        // 传入值是Double
        {
            Record r = new DefaultRecord();
            r.addColumn(new DoubleColumn(-9012.023));
            r.addColumn(new DoubleColumn(-9012.023));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("-9012.023"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("-9012.023"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromLong(0), ts++)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new DoubleColumn(0));
            r.addColumn(new DoubleColumn(0));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("0"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromLong(0), ts++)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new DoubleColumn(1211.12));
            r.addColumn(new DoubleColumn(1211.12));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("1211.12"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("1211.12"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromLong(0), ts++)
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
        testWithNoTS(ots,conf, rs, expect);
    }
    // 传入值是Bool，用户指定的是String, 期待转换正常，且值符合预期
    @Test
    public void testBoolToString() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        long ts = 1;
        {
            Record r = new DefaultRecord();
            r.addColumn(new BoolColumn(true));
            r.addColumn(new BoolColumn(true));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("true"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("true"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromLong(0), ts++)
                    .toRow();
            expect.add(row);
        }
        {
            Record r = new DefaultRecord();
            r.addColumn(new BoolColumn(false));
            r.addColumn(new BoolColumn(false));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("false"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("false"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromLong(0), ts++)
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
        testWithNoTS(ots,conf, rs, expect);
    }
    // 传入值是Binary，用户指定的是String, 期待转换正常，且值符合预期
    @Test
    public void testBinaryToString() throws Exception {
        
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        long ts = 1;
        {
            Record r = new DefaultRecord();
            r.addColumn(new BytesColumn("试试~，。1？！@#￥%……&*（）——+".getBytes("UTF-8")));
            r.addColumn(new BytesColumn("试试~，。1？！@#￥%……&*（）——+".getBytes("UTF-8")));
            r.addColumn(new LongColumn(0));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("试试~，。1？！@#￥%……&*（）——+"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("试试~，。1？！@#￥%……&*（）——+"))
                    .addAttrColumn(getColumnName(0), ColumnValue.fromLong(0), ts++)
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
        testWithNoTS(ots,conf, rs, expect);
    }
}
