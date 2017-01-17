package com.alibaba.datax.plugin.writer.otswriter.unittest;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.openservices.ots.internal.MockOTSClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.otswriter.common.Conf;
import com.alibaba.datax.plugin.writer.otswriter.common.DataChecker;
import com.alibaba.datax.plugin.writer.otswriter.common.OTSHelper;
import com.alibaba.datax.plugin.writer.otswriter.common.OTSRowBuilder;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.common.Utils;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector.RecordAndMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSLine;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSSendBuffer;
import com.alibaba.datax.plugin.writer.otswriter.utils.CollectorUtil;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.alibaba.datax.plugin.writer.otswriter.utils.ParseRecord;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.Row;

/**
 * 测试目的：测试在Record在任意位置为空的情况
 * 
 * 测试场景：
 * 1.列的类型
 *      1.1 PK
 *      1.2 ATTR
 *      
 * 2.列的类型
 *      1.1 STRING
 *      1.2 INT
 *      1.3 DOUBLE
 *      1.4 BOOLEAN
 *      1.5 BINARY
 */
public class NullColumnForNormalUnittest {
    
    private static String tableName = "NullColumnForNormalUnittest";
    
    private static final Logger LOG = LoggerFactory.getLogger(NullColumnForNormalUnittest.class);
    
    @BeforeClass
    public static void init() {}
    
    @AfterClass
    public static void close() {}
    
    public static void test(
            Map<String, PrimaryKeyType> pk, 
            Map<String, ColumnType> attr, 
            List<Record> input, 
            List<Row> expect
            ) throws Exception {
        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.PUT_ROW);
        conf.setRetry(5);
        conf.setEncodePkColumnMapping(Utils.getPkColumnMapping(conf.getPrimaryKeyColumn()));
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        MockOTSClient ots = new MockOTSClient(10000, null, null);
        CollectorUtil.init(collector);
        OTSSendBuffer buffer = new OTSSendBuffer(ots, conf);
        
        for (Record r :  input) {
            OTSLine line = ParseRecord.parseNormalRecordToOTSLine(
                    conf.getTableName(), 
                    conf.getOperation(), 
                    Common.getPkColumnMapping(conf.getEncodePkColumnMapping()),  
                    conf.getAttributeColumn(), 
                    r,
                    conf.getTimestamp());
            buffer.write(line);
        }
        buffer.close();
        
        assertEquals(0, collector.getContent().size());
        assertEquals(true, DataChecker.checkRow(OTSHelper.getAllData(ots, conf), expect, false));
    }
    
    public static void testIllegal(
            Map<String, PrimaryKeyType> pk, 
            Map<String, ColumnType> attr, 
            List<Record> input, 
            List<RecordAndMessage> expect) throws Exception {
        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.PUT_ROW);
        conf.setRetry(5);
        conf.setEncodePkColumnMapping(Utils.getPkColumnMapping(conf.getPrimaryKeyColumn()));
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        MockOTSClient ots = new MockOTSClient(5000, null, null);
        CollectorUtil.init(collector);
        OTSSendBuffer buffer = new OTSSendBuffer(ots, conf);
        
        for (Record r :  input) {
            OTSLine line = ParseRecord.parseNormalRecordToOTSLine(
                    conf.getTableName(), 
                    conf.getOperation(), 
                    Common.getPkColumnMapping(conf.getEncodePkColumnMapping()), 
                    conf.getAttributeColumn(), 
                    r,
                    conf.getTimestamp());
            if (line != null) {
                buffer.write(line);
            }
        }
        buffer.close();
        
        assertEquals(expect.size(), collector.getContent().size());
        assertEquals(true, DataChecker.checkRecordWithMessage(collector.getContent(), expect)); 
    }
    
    class TestInvalidParam {
        public Map<String, PrimaryKeyType> pk = new LinkedHashMap<String, PrimaryKeyType>();
        public Map<String, ColumnType> attr = new LinkedHashMap<String, ColumnType>();
        public List<Record> input = new ArrayList<Record>();
        public List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
    }
    
    class TestValidParam {
        public Map<String, PrimaryKeyType> pk = new LinkedHashMap<String, PrimaryKeyType>();
        public Map<String, ColumnType> attr = new LinkedHashMap<String, ColumnType>();
        public List<Record> input = new ArrayList<Record>();
        public List<Row> expect = new ArrayList<Row>();
    }
    
    @Test
    public void testAllForPK() throws Exception {
        // prepare
        List<TestInvalidParam> ps = new ArrayList<TestInvalidParam>();
        // string
        {
            TestInvalidParam p = new TestInvalidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);
            p.pk.put("pk_1", PrimaryKeyType.STRING);
            
            p.attr.put("attr_0", ColumnType.STRING);
            
            {
                Record record = new DefaultRecord();
                record.addColumn(new StringColumn());
                record.addColumn(new StringColumn(""));
                record.addColumn(new StringColumn(""));
                
                p.input.add(record);
                
                p.expect.add(new RecordAndMessage(record, "The column of record is NULL, primary key name : pk_0 ."));
            }
            ps.add(p);
        }
        
        // int
        {
            TestInvalidParam p = new TestInvalidParam();
            p.pk.put("pk_0", PrimaryKeyType.INTEGER);
            p.pk.put("pk_1", PrimaryKeyType.INTEGER);
            
            p.attr.put("attr_0", ColumnType.STRING);
            
            {
                Record record = new DefaultRecord();
                record.addColumn(new LongColumn(100));
                record.addColumn(new LongColumn());
                record.addColumn(new StringColumn(""));
                
                p.input.add(record);
                
                p.expect.add(new RecordAndMessage(record, "The column of record is NULL, primary key name : pk_1 ."));
            }
            ps.add(p);
        }
        
        // binary
        {
            TestInvalidParam p = new TestInvalidParam();
            p.pk.put("pk_0", PrimaryKeyType.BINARY);
            p.pk.put("pk_1", PrimaryKeyType.BINARY);
            
            p.attr.put("attr_0", ColumnType.STRING);
            
            {
                Record record = new DefaultRecord();
                record.addColumn(new BytesColumn());
                record.addColumn(new BytesColumn("".getBytes("UTF-8")));
                record.addColumn(new StringColumn(""));
                
                p.input.add(record);
                
                p.expect.add(new RecordAndMessage(record, "The column of record is NULL, primary key name : pk_0 ."));
            }
            ps.add(p);
        }
        
        // check
        int index = 0;
        for (TestInvalidParam tp : ps) {
            LOG.info("TestParam Index : {}", index++);
            testIllegal(tp.pk, tp.attr, tp.input, tp.expect);
        }
    }
    
    @Test
    public void testAllForATTR() throws Exception {
        // prepare
        List<TestValidParam> ps = new ArrayList<TestValidParam>();
        // string
        {
            TestValidParam p = new TestValidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);

            p.attr.put("attr_0", ColumnType.STRING);
            p.attr.put("attr_1", ColumnType.STRING);
            
            {
                Record record = new DefaultRecord();
                record.addColumn(new StringColumn(""));
                record.addColumn(new StringColumn(""));
                record.addColumn(new StringColumn());
                p.input.add(record);
                
                Row row = OTSRowBuilder.newInstance()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(""))
                        .addAttrColumn("attr_0", ColumnValue.fromString(""), 1)
                        .toRow();
                
               p.expect.add(row);
            }
            ps.add(p);
        }
        
        // int
        {
            TestValidParam p = new TestValidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);

            p.attr.put("attr_0", ColumnType.INTEGER);
            p.attr.put("attr_1", ColumnType.INTEGER);
            
            {
                Record record = new DefaultRecord();
                record.addColumn(new StringColumn(""));
                record.addColumn(new LongColumn());
                record.addColumn(new LongColumn(100));
                p.input.add(record);
                
                Row row = OTSRowBuilder.newInstance()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(""))
                        .addAttrColumn("attr_1", ColumnValue.fromLong(100), 1)
                        .toRow();
                
               p.expect.add(row);
            }
            ps.add(p);
        }
        
        // binary
        {
            TestValidParam p = new TestValidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);

            p.attr.put("attr_0", ColumnType.BINARY);
            p.attr.put("attr_1", ColumnType.BINARY);
            
            {
                Record record = new DefaultRecord();
                record.addColumn(new StringColumn(""));
                
                record.addColumn(new BytesColumn("".getBytes("UTF-8")));
                record.addColumn(new BytesColumn());
                p.input.add(record);
                
                Row row = OTSRowBuilder.newInstance()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(""))
                        .addAttrColumn("attr_0", ColumnValue.fromBinary("".getBytes("UTF-8")), 1)
                        .toRow();
                
               p.expect.add(row);
            }
            ps.add(p);
        }
        
        // double
        {
            TestValidParam p = new TestValidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);

            p.attr.put("attr_0", ColumnType.DOUBLE);
            p.attr.put("attr_1", ColumnType.DOUBLE);
            
            {
                Record record = new DefaultRecord();
                record.addColumn(new StringColumn(""));
                record.addColumn(new DoubleColumn());
                record.addColumn(new DoubleColumn(-111.02));
                p.input.add(record);
                
                Row row = OTSRowBuilder.newInstance()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(""))
                        .addAttrColumn("attr_1", ColumnValue.fromDouble(-111.02), 1)
                        .toRow();
                
               p.expect.add(row);
            }
            ps.add(p);
        }
        
        // boolean
        {
            TestValidParam p = new TestValidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);

            p.attr.put("attr_0", ColumnType.BOOLEAN);
            p.attr.put("attr_1", ColumnType.BOOLEAN);
            p.attr.put("attr_2", ColumnType.BOOLEAN);
            p.attr.put("attr_3", ColumnType.BOOLEAN);
            
            {
                Record record = new DefaultRecord();
                record.addColumn(new StringColumn(""));
                
                record.addColumn(new BoolColumn(true));
                record.addColumn(new BoolColumn());
                record.addColumn(new BoolColumn(false));
                record.addColumn(new BoolColumn());
                p.input.add(record);
                
                Row row = OTSRowBuilder.newInstance()
                        .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(""))
                        .addAttrColumn("attr_0", ColumnValue.fromBoolean(true), 1)
                        .addAttrColumn("attr_2", ColumnValue.fromBoolean(false), 1)
                        .toRow();
                
               p.expect.add(row);
            }
            ps.add(p);
        }
        
        // check
        int index = 0;
        for (TestValidParam tp : ps) {
            LOG.info("TestParam Index : {}", index++);
            test(tp.pk, tp.attr, tp.input, tp.expect);
        }
    }
}
