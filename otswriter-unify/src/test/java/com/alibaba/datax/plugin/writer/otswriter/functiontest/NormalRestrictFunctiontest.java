package com.alibaba.datax.plugin.writer.otswriter.functiontest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.datax.common.element.BytesColumn;
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
 * 限制项相关的测试
 */
public class NormalRestrictFunctiontest extends BaseTest{
    
    private static String tableName = "ots_writer_normal_restrict_ft";
    
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
        tableMeta.addPrimaryKeyColumn("pk_1", PrimaryKeyType.BINARY);

        OTSHelper.createTableSafe(ots, tableMeta);
    }
    
    @After
    public void teardown() {}
    
    // PK的String Column的值等于1KB
    @Test
    public void testPKString1024B() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1024; i++) {
            sb.append("c");
        }
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(sb.toString()));
            r.addColumn(new BytesColumn("0".getBytes()));
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn("0".getBytes()));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(sb.toString()))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromBinary("0".getBytes()))
                    
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString("0"), 1)
                    .addAttrColumn(getColumnName(1), ColumnValue.fromBinary("0".getBytes()), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.STRING);
        columns.put(getColumnName(1), ColumnType.BINARY);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots, conf, rs, expect);
    }
    // PK的String Column的值大于1KB
    @Test
    public void testPKString1025B() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1025; i++) {
            sb.append("c");
        }
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(sb.toString()));
            r.addColumn(new BytesColumn("0".getBytes()));
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn("0".getBytes()));
            rs.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.");
            rms.add(rm);
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.STRING);
        columns.put(getColumnName(1), ColumnType.BINARY);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        test(ots, conf, rs, null, rms, false);
    }
    // Attr的String Column的值等于64KB
    @Test
    public void testAttrString64KB() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 64*1024; i++) {
            sb.append("a");
        }
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn("0".getBytes()));
            r.addColumn(new StringColumn(sb.toString()));
            r.addColumn(new BytesColumn("0".getBytes()));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromBinary("0".getBytes()))
                    
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString(sb.toString()), 1)
                    .addAttrColumn(getColumnName(1), ColumnValue.fromBinary("0".getBytes()), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.STRING);
        columns.put(getColumnName(1), ColumnType.BINARY);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots, conf, rs, expect);
    }
    
    // Attr的String Column的值大于64KB
    @Test
    public void testAttrStringMoreThan64KB() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (2*1024*1024+1); i++) {
            sb.append("a");
        }
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn("0".getBytes()));
            r.addColumn(new StringColumn(sb.toString()));
            r.addColumn(new BytesColumn("0".getBytes()));
            rs.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "The length of attribute column: 'attr_000000' exceeds the MaxLength:2097152 with CurrentLength:2097153.");
            rms.add(rm);
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.STRING);
        columns.put(getColumnName(1), ColumnType.BINARY);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        test(ots, conf, rs, null, rms, false);
    }
    
    // PK的Binary Column的值等于1KB
    @Test
    public void testPKBinary1024B() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1024; i++) {
            sb.append("c");
        }
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn(sb.toString().getBytes()));
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn("0".getBytes()));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromBinary(sb.toString().getBytes()))
                    
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString("0"), 1)
                    .addAttrColumn(getColumnName(1), ColumnValue.fromBinary("0".getBytes()), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.STRING);
        columns.put(getColumnName(1), ColumnType.BINARY);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots, conf, rs, expect);
    }
    // PK的Binary Column的值大于1KB
    @Test
    public void testPKBinary1025B() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1025; i++) {
            sb.append("c");
        }
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(sb.toString()));
            r.addColumn(new BytesColumn("0".getBytes()));
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn("0".getBytes()));
            rs.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "The length of primary key column: 'pk_0' exceeds the MaxLength:1024 with CurrentLength:1025.");
            rms.add(rm);
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.STRING);
        columns.put(getColumnName(1), ColumnType.BINARY);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        test(ots, conf, rs, null, rms, false);
    }
    // Attr的Binary Column的值等于64KB
    @Test
    public void testAttrBinary64KB() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 64*1024; i++) {
            sb.append("a");
        }
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn("0".getBytes()));
            r.addColumn(new StringColumn(sb.toString()));
            r.addColumn(new BytesColumn("0".getBytes()));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromBinary("0".getBytes()))
                    
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString(sb.toString()), 1)
                    .addAttrColumn(getColumnName(1), ColumnValue.fromBinary("0".getBytes()), 1)
                    .toRow();
            expect.add(row);
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.STRING);
        columns.put(getColumnName(1), ColumnType.BINARY);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        testWithNoTS(ots, conf, rs, expect);
    }
    
    // Attr的Binary Column的值大于64KB
    @Test
    public void testAttrBinaryMoreThan64KB() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (2 * 1024 *1024+1); i++) {
            sb.append("a");
        }
        
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn("0".getBytes()));
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn(sb.toString().getBytes()));
            rs.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "The length of attribute column: 'attr_000001' exceeds the MaxLength:2097152 with CurrentLength:2097153.");
            rms.add(rm);
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.STRING);
        columns.put(getColumnName(1), ColumnType.BINARY);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.NORMAL);
        test(ots, conf, rs, null, rms, false);
    }
}
