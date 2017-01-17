package com.alibaba.datax.plugin.writer.otswriter.functiontest;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.datax.common.element.BytesColumn;
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

public class NormalForExceptionFunctiontest extends BaseTest{
    private static String tableName = "NormalForExceptionFunctiontest";
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
        tableMeta.addPrimaryKeyColumn("Uid", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("Pid", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("Mid", PrimaryKeyType.BINARY);

        OTSHelper.createTableSafe(ots, tableMeta);
    }
    
    @After
    public void teardown() {}
    
    /**
     * 测试目的：测试datax传入了不符合预期的数据的ots-writer的行为表现是否符合预期。
     * 测试内容：用户配置了5列，但是datax给了4列，期望writer异常退出，错误消息符合预期
     * @throws Exception 
     */
    @Test
    public void testCase1() throws Exception {
        List<Record> input = new ArrayList<Record>();
        // 构造数据
        {
            for (int j = 0; j < 9; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(j));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));
                
                // columnName
                r.addColumn(new LongColumn(j));
                r.addColumn(new LongColumn(j));
                input.add(r);
            }
            for (int j = 9; j < 10; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(j));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));
                // 少一个PK
                
                // columnName
                r.addColumn(new LongColumn(j));
                input.add(r);
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(2, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        test(ots, conf, input, "Size of record not equal size of config column. record size : 4, config column size : 5, record data : {\"data\":[{\"byteSize\":9,\"rawData\":\"Uid_value\",\"type\":\"STRING\"},{\"byteSize\":8,\"rawData\":9,\"type\":\"LONG\"},{\"byteSize\":9,\"rawData\":\"TWlkX3ZhbHVl\",\"type\":\"BYTES\"},{\"byteSize\":8,\"rawData\":9,\"type\":\"LONG\"}],\"size\":4}.");
    }
    
    /**
     * 测试目的：测试datax传入了不符合预期的数据的ots-writer的行为表现是否符合预期。
     * 测试内容：用户配置了5列，但是datax给了6列，期望writer异常退出，错误消息符合预期
     * @throws Exception
     */
    @Test
    public void testCase2() throws Exception {
        List<Record> input = new ArrayList<Record>();
        // 构造数据
        {
            for (int j = 0; j < 9; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(j));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));
                
                // columnName
                r.addColumn(new LongColumn(j));
                r.addColumn(new LongColumn(j));
                input.add(r);
            }
            for (int j = 9; j < 10; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(j));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));
                
                // columnName
                r.addColumn(new LongColumn(j));
                r.addColumn(new LongColumn(j));
                r.addColumn(new LongColumn(j));
                input.add(r);
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(2, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        test(ots, conf, input, "Size of record not equal size of config column. record size : 6, config column size : 5, record data : {\"data\":[{\"byteSize\":9,\"rawData\":\"Uid_value\",\"type\":\"STRING\"},{\"byteSize\":8,\"rawData\":9,\"type\":\"LONG\"},{\"byteSize\":9,\"rawData\":\"TWlkX3ZhbHVl\",\"type\":\"BYTES\"},{\"byteSize\":8,\"rawData\":9,\"type\":\"LONG\"},{\"byteSize\":8,\"rawData\":9,\"type\":\"LONG\"},{\"byteSize\":8,\"rawData\":9,\"type\":\"LONG\"}],\"size\":6}.");
    }
    
    /**
     * 测试目的：测试datax传入了不符合预期的数据的ots-writer的行为表现是否符合预期。
     * 测试内容：构造了10行数据，但是其中有一行数据的PK列为空，期望该行数据被记录到脏数据回收器中，错误消息符合预期
     * @throws Exception
     */
    @Test
    public void testCase3() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<RecordAndMessage> rm = new ArrayList<RecordAndMessage>();
        // 构造数据
        {
            for (int j = 0; j < 9; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(j));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));
                
                r.addColumn(new LongColumn(j));
                r.addColumn(new LongColumn(j));
                
                input.add(r);
                
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("Uid_value"));
                row.addPrimaryKeyColumn("Pid", PrimaryKeyValue.fromLong(j));
                row.addPrimaryKeyColumn("Mid", PrimaryKeyValue.fromBinary("Mid_value".getBytes()));
                row.addAttrColumn(getColumnName(0), ColumnValue.fromLong(j), j);
                row.addAttrColumn(getColumnName(1), ColumnValue.fromLong(j), j);
                expect.add(row.toRow());
            }

            for (int j = 9; j < 10; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn());
                r.addColumn(new BytesColumn("Mid_value".getBytes()));
                
                r.addColumn(new LongColumn(j));
                r.addColumn(new LongColumn(j));
                
                input.add(r);
                rm.add(new RecordAndMessage(r, "The column of record is NULL, primary key name : Pid ."));
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(2, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        conf.setBatchWriteCount(1);
        conf.setConcurrencyWrite(1);
        test(ots, conf, input, expect, rm, false);
    }
    
    /**
     * 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。
     * 测试内容：用户配置3列PK，构造10个Cell，其中一个Cell只传入3列PK，但是有一个PK列不能成功的转换为指定的类型，期望该Cell被记录到脏数据回收器中，错误消息符合预期
     * @throws Exception
     */
    @Test
    public void testCase4() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<RecordAndMessage> rm = new ArrayList<RecordAndMessage>();
        // 构造数据
        {
            for (int j = 0; j < 9; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(j));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));
                
                r.addColumn(new LongColumn(j));
                r.addColumn(new LongColumn(j));
                
                input.add(r);
                
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                row.addPrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("Uid_value"));
                row.addPrimaryKeyColumn("Pid", PrimaryKeyValue.fromLong(j));
                row.addPrimaryKeyColumn("Mid", PrimaryKeyValue.fromBinary("Mid_value".getBytes()));
                row.addAttrColumn(getColumnName(0), ColumnValue.fromLong(j), j);
                row.addAttrColumn(getColumnName(1), ColumnValue.fromLong(j), j);
                expect.add(row.toRow());
            }

            for (int j = 9; j < 10; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(j));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));
                
                r.addColumn(new LongColumn(j));
                r.addColumn(new StringColumn("hello world"));
                
                input.add(r);
                rm.add(new RecordAndMessage(r, "Column coversion error, src type : STRING, src value: hello world, expect type: INTEGER ."));
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                getColumnMeta(2, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        conf.setBatchWriteCount(1);
        conf.setConcurrencyWrite(1);
        test(ots, conf, input, expect, rm, false);
    }
}
