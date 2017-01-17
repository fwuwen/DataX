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
 * 限制项相关的测试
 */
public class MultiVersionRestrictFunctiontest extends BaseTest{
    
    private static String tableName = "ots_writer_multiversion_restrict_ft";
    
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
        long ts = System.currentTimeMillis();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(sb.toString()));
            r.addColumn(new BytesColumn("0".getBytes()));
            r.addColumn(new StringColumn(getColumnName(0)));
            r.addColumn(new LongColumn(ts));
            r.addColumn(new StringColumn("0"));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(sb.toString()))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromBinary("0".getBytes()))
                    
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString("0"), ts)
                    .toRow();
            expect.add(row);
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.STRING);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, rs, expect);
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
        long ts = System.currentTimeMillis();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(sb.toString()));
            r.addColumn(new BytesColumn("0".getBytes()));
            r.addColumn(new StringColumn(getColumnName(0)));
            r.addColumn(new LongColumn(ts));
            r.addColumn(new StringColumn("0"));
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
                OTSMode.MULTI_VERSION);
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
        long ts = System.currentTimeMillis();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn("0".getBytes()));
            r.addColumn(new StringColumn(getColumnName(0)));
            r.addColumn(new LongColumn(ts));
            r.addColumn(new StringColumn(sb.toString()));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromBinary("0".getBytes()))
                    
                    .addAttrColumn(getColumnName(0), ColumnValue.fromString(sb.toString()), ts)
                    .toRow();
            expect.add(row);
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.STRING);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, rs, expect);
    }
    
    // Attr的String Column的值大于64KB
    @Test
    public void testAttrStringMoreThan64KB() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (2 * 1024 *1024+1); i++) {
            sb.append("a");
        }
        long ts = System.currentTimeMillis();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn("0".getBytes()));
            r.addColumn(new StringColumn(getColumnName(0)));
            r.addColumn(new LongColumn(ts));
            r.addColumn(new StringColumn(sb.toString()));
            rs.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "The length of attribute column: 'attr_000000' exceeds the MaxLength:2097152 with CurrentLength:2097153.");
            rms.add(rm);
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.STRING);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
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
        long ts = System.currentTimeMillis();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn(sb.toString().getBytes()));
            r.addColumn(new StringColumn(getColumnName(0)));
            r.addColumn(new LongColumn(ts));
            r.addColumn(new BytesColumn("0".getBytes()));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromBinary(sb.toString().getBytes()))
                    
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBinary("0".getBytes()), ts)
                    .toRow();
            expect.add(row);
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.BINARY);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, rs, expect);
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
        long ts = System.currentTimeMillis();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn(sb.toString().getBytes()));
            r.addColumn(new StringColumn(getColumnName(0)));
            r.addColumn(new LongColumn(ts));
            r.addColumn(new BytesColumn("0".getBytes()));
            rs.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "The length of primary key column: 'pk_1' exceeds the MaxLength:1024 with CurrentLength:1025.");
            rms.add(rm);
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.BINARY);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
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
        long ts = System.currentTimeMillis();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn("0".getBytes()));
            r.addColumn(new StringColumn(getColumnName(0)));
            r.addColumn(new LongColumn(ts));
            r.addColumn(new BytesColumn(sb.toString().getBytes()));
            rs.add(r);
            
            Row row = OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("0"))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromBinary("0".getBytes()))
                    
                    .addAttrColumn(getColumnName(0), ColumnValue.fromBinary(sb.toString().getBytes()), ts)
                    .toRow();
            expect.add(row);
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.BINARY);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, rs, expect);
    }
    
    // Attr的Binary Column的值大于64KB
    @Test
    public void testAttrBinaryMoreThan64KB() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<RecordAndMessage> rms = new ArrayList<RecordAndMessage>();
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (2 * 1024 * 1024 + 1); i++) {
            sb.append("a");
        }
        long ts = System.currentTimeMillis();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("0"));
            r.addColumn(new BytesColumn("0".getBytes()));
            r.addColumn(new StringColumn(getColumnName(0)));
            r.addColumn(new LongColumn(ts));
            r.addColumn(new StringColumn(sb.toString()));
            rs.add(r);
            
            RecordAndMessage rm = new RecordAndMessage(r, "The length of attribute column: 'attr_000000' exceeds the MaxLength:2097152 with CurrentLength:2097153.");
            rms.add(rm);
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.BINARY);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        test(ots, conf, rs, null, rms, false);
    }
    
    private String getString(char c, int size) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(c);
        }
        return sb.toString();
    }
    
    // 构造10行数据，共计大小为1M，期望数据被正常写入OTS中，结果符合预期
    @Test
    public void testRequest1M() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();

        long ts = System.currentTimeMillis();
        // 10
        for (int i = 0; i < 9; i++) {
            String value = String.format("%02d", i);
            OTSRowBuilder builder = OTSRowBuilder.newInstance();
            builder.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(value));
            builder.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromBinary(value.getBytes()));
            // cell one
            {
                Record r = new DefaultRecord();
                
                r.addColumn(new StringColumn(value));
                r.addColumn(new BytesColumn(value.getBytes()));
                r.addColumn(new StringColumn(getColumnName(0)));
                r.addColumn(new LongColumn(ts));
                r.addColumn(new StringColumn(getString((char)('a' + i), 64 * 1024)));
                rs.add(r);
                builder.addAttrColumn(getColumnName(0), ColumnValue.fromString(getString((char)('a' + i), 64 * 1024)), ts);
            }
            // cell two
            {
                Record r = new DefaultRecord();
                
                r.addColumn(new StringColumn(value));
                r.addColumn(new BytesColumn(value.getBytes()));
                r.addColumn(new StringColumn(getColumnName(1)));
                r.addColumn(new LongColumn(ts));
                r.addColumn(new StringColumn(getString((char)('a' + i),39271)));
                rs.add(r);
                builder.addAttrColumn(getColumnName(1), ColumnValue.fromString(getString((char)('a' + i),39271)), ts);
            }
            expect.add(builder.toRow());
        }
        // 1
        for (int i = 9; i < 10; i++) {
            String value = String.format("%02d", i);
            OTSRowBuilder builder = OTSRowBuilder.newInstance();
            builder.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(value));
            builder.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromBinary(value.getBytes()));
            // cell one
            {
                Record r = new DefaultRecord();
                
                r.addColumn(new StringColumn(value));
                r.addColumn(new BytesColumn(value.getBytes()));
                r.addColumn(new StringColumn(getColumnName(0)));
                r.addColumn(new LongColumn(ts));
                r.addColumn(new StringColumn(getString((char)('a' + i), 64 * 1024)));
                rs.add(r);
                builder.addAttrColumn(getColumnName(0), ColumnValue.fromString(getString((char)('a' + i), 64 * 1024)), ts);
            }
            // cell two
            {
                Record r = new DefaultRecord();
                
                r.addColumn(new StringColumn(value));
                r.addColumn(new BytesColumn(value.getBytes()));
                r.addColumn(new StringColumn(getColumnName(1)));
                r.addColumn(new LongColumn(ts));
                r.addColumn(new StringColumn(getString((char)('a' + i),39277)));
                rs.add(r);
                builder.addAttrColumn(getColumnName(1), ColumnValue.fromString(getString((char)('a' + i), 39277)), ts);
            }
            expect.add(builder.toRow());
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.STRING);
        columns.put(getColumnName(1), ColumnType.STRING);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, rs, expect);
    }
    
    // 构造10 + 1行数据，共计大小为1M + 1Byte，期望数据被正常写入OTS中，结果符合预期
    // 104857 * 9 + 104863 * 1 = 1M + 1B = 1048576 + 1
    // 104857 = pk_0(4) + 2 + pk_1(4) + 2 + ({columnName}(11) + {ts}(8) + {value}(64KB))(65555) + ({columnName}(11) + {ts}(8) + {value}(39271))(39290)   * 9
    // 104864 = pk_0(4) + 2 + pk_1(4) + 2 + ({columnName}(11) + {ts}(8) + {value}(64KB))(65555) + ({columnName}(11) + {ts}(8) + {value}(39278))(39297)   * 1
    @Test
    public void testRequest1M_1Byte() throws Exception {
        List<Record> rs = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();

        long ts = System.currentTimeMillis();
        // 10
        for (int i = 0; i < 9; i++) {
            String value = String.format("%02d", i);
            OTSRowBuilder builder = OTSRowBuilder.newInstance();
            builder.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(value));
            builder.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromBinary(value.getBytes()));
            // cell one
            {
                Record r = new DefaultRecord();
                
                r.addColumn(new StringColumn(value));
                r.addColumn(new BytesColumn(value.getBytes()));
                r.addColumn(new StringColumn(getColumnName(0)));
                r.addColumn(new LongColumn(ts));
                r.addColumn(new StringColumn(getString((char)('a' + i), 64 * 1024)));
                rs.add(r);
                builder.addAttrColumn(getColumnName(0), ColumnValue.fromString(getString((char)('a' + i), 64 * 1024)), ts);
            }
            // cell two
            {
                Record r = new DefaultRecord();
                
                r.addColumn(new StringColumn(value));
                r.addColumn(new BytesColumn(value.getBytes()));
                r.addColumn(new StringColumn(getColumnName(1)));
                r.addColumn(new LongColumn(ts));
                r.addColumn(new StringColumn(getString((char)('a' + i),39271)));
                rs.add(r);
                builder.addAttrColumn(getColumnName(1), ColumnValue.fromString(getString((char)('a' + i),39271)), ts);
            }
            expect.add(builder.toRow());
        }
        // 1
        for (int i = 9; i < 10; i++) {
            String value = String.format("%02d", i);
            OTSRowBuilder builder = OTSRowBuilder.newInstance();
            builder.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(value));
            builder.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromBinary(value.getBytes()));
            // cell one
            {
                Record r = new DefaultRecord();
                
                r.addColumn(new StringColumn(value));
                r.addColumn(new BytesColumn(value.getBytes()));
                r.addColumn(new StringColumn(getColumnName(0)));
                r.addColumn(new LongColumn(ts));
                r.addColumn(new StringColumn(getString((char)('a' + i), 64 * 1024)));
                rs.add(r);
                builder.addAttrColumn(getColumnName(0), ColumnValue.fromString(getString((char)('a' + i), 64 * 1024)), ts);
            }
            // cell two
            {
                Record r = new DefaultRecord();
                
                r.addColumn(new StringColumn(value));
                r.addColumn(new BytesColumn(value.getBytes()));
                r.addColumn(new StringColumn(getColumnName(1)));
                r.addColumn(new LongColumn(ts));
                r.addColumn(new StringColumn(getString((char)('a' + i),39278)));
                rs.add(r);
                builder.addAttrColumn(getColumnName(1), ColumnValue.fromString(getString((char)('a' + i), 39278)), ts);
            }
            expect.add(builder.toRow());
        }
        // check
        Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
        columns.put(getColumnName(0), ColumnType.STRING);
        columns.put(getColumnName(1), ColumnType.STRING);
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                columns,
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, rs, expect);
    }
}
