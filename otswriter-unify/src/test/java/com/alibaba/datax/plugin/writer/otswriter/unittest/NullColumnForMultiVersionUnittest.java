package com.alibaba.datax.plugin.writer.otswriter.unittest;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector.RecordAndMessage;
import com.alibaba.datax.plugin.writer.otswriter.common.Utils;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSMode;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.utils.CollectorUtil;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.alibaba.datax.plugin.writer.otswriter.utils.ParseRecord;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;


/**
 * 测试目的：测试在Record在任意位置为空的情况
 * 
 * 测试场景：
 * 1.列的类型
 *      1.1 PK
 *      1.2 ColumnName
 *      1.3 TS
 *      1.4 Value
 *      
 * 2.列的类型
 *      1.1 STRING
 *      1.2 INT
 *      1.3 DOUBLE
 *      1.4 BOOLEAN
 *      1.5 BINARY
 */
public class NullColumnForMultiVersionUnittest {
    private static String tableName = "NullColumnForMultiVersionUnittest";
    
    private static final Logger LOG = LoggerFactory.getLogger(NullColumnForMultiVersionUnittest.class);
    
    @BeforeClass
    public static void init() {}
    
    @AfterClass
    public static void close() {}
    
    class TestInvalidParam {
        public Map<String, PrimaryKeyType> pk = new LinkedHashMap<String, PrimaryKeyType>();
        public Map<String, ColumnType> attr = new LinkedHashMap<String, ColumnType>();
        public List<Record> input = new ArrayList<Record>();
        public List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
    }
    
    public static void testIllegal(
            Map<String, PrimaryKeyType> pk, 
            Map<String, ColumnType> attr, 
            List<Record> input, 
            List<RecordAndMessage> expect) throws Exception {
        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.UPDATE_ROW);
        conf.setRetry(5);
        conf.setMode(OTSMode.MULTI_VERSION);
        conf.setEncodePkColumnMapping(Utils.getPkColumnMapping(conf.getPrimaryKeyColumn()));
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        CollectorUtil.init(collector);
        
        for (Record r : input) {
            PrimaryKey pKey = null;
            if ((pKey = Common.getPKFromRecord(Common.getPkColumnMapping(conf.getEncodePkColumnMapping()), r)) != null) {
                ParseRecord.parseMultiVersionRecordToOTSLine(
                        conf.getTableName(), 
                        conf.getOperation(), 
                        Common.getPkColumnMapping(conf.getEncodePkColumnMapping()), 
                        null, 
                        pKey,
                        input);
            }
        }
        
        assertEquals(expect.size(), collector.getContent().size());
        assertEquals(true, DataChecker.checkRecordWithMessage(collector.getContent(), expect)); 
    }
    
    @Test
    public void testForPK() throws Exception {
        
        // prepare
        List<TestInvalidParam> ps = new ArrayList<TestInvalidParam>();
        
        {
            TestInvalidParam p = new TestInvalidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);
            p.pk.put("pk_1", PrimaryKeyType.INTEGER);
            p.pk.put("pk_2", PrimaryKeyType.BINARY);
            
            p.attr.put("attr_0", ColumnType.STRING);
            p.attr.put("attr_1", ColumnType.STRING);
            
            // Row
            {
                {
                    Record record = new DefaultRecord();
                    record.addColumn(new StringColumn()); // pk 0
                    record.addColumn(new LongColumn(1000)); // pk 1
                    record.addColumn(new BytesColumn("".getBytes("UTF-8"))); // pk 2
                    
                    record.addColumn(new StringColumn("attr_0")); // cn
                    record.addColumn(new StringColumn("14922213432")); // ts
                    record.addColumn(new StringColumn("hello")); // value
                    
                    p.input.add(record);
                    
                    p.expect.add(
                            new RecordAndMessage(
                                    record, 
                                    "The column of record is NULL, primary key name : pk_0 ."
                                    )
                            );
                }
                
                {
                    Record record = new DefaultRecord();
                    record.addColumn(new StringColumn()); // pk 0
                    record.addColumn(new LongColumn(1000)); // pk 1
                    record.addColumn(new BytesColumn("".getBytes("UTF-8"))); // pk 2
                    
                    record.addColumn(new StringColumn("attr_1")); // cn
                    record.addColumn(new StringColumn("14922213432")); // ts
                    record.addColumn(new StringColumn("world")); // value
                    
                    p.input.add(record);
                    
                    p.expect.add(
                            new RecordAndMessage(
                                    record, 
                                    "The column of record is NULL, primary key name : pk_0 ."
                                    )
                            );
                }
                ps.add(p);
            }
        }
        
        {
            TestInvalidParam p = new TestInvalidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);
            p.pk.put("pk_1", PrimaryKeyType.INTEGER);
            p.pk.put("pk_2", PrimaryKeyType.BINARY);
            
            p.attr.put("attr_0", ColumnType.STRING);
            p.attr.put("attr_1", ColumnType.STRING);
            
            // Row
            {
                {
                    Record record = new DefaultRecord();
                    record.addColumn(new StringColumn("a")); // pk 0
                    record.addColumn(new LongColumn()); // pk 1
                    record.addColumn(new BytesColumn("".getBytes("UTF-8"))); // pk 2
                    
                    record.addColumn(new StringColumn("attr_0")); // cn
                    record.addColumn(new StringColumn("14922213432")); // ts
                    record.addColumn(new StringColumn("hello")); // value
                    
                    p.input.add(record);
                    
                    p.expect.add(
                            new RecordAndMessage(
                                    record, 
                                    "The column of record is NULL, primary key name : pk_1 ."
                                    )
                            );
                }
                
                {
                    Record record = new DefaultRecord();
                    record.addColumn(new StringColumn("a")); // pk 0
                    record.addColumn(new LongColumn()); // pk 1
                    record.addColumn(new BytesColumn("".getBytes("UTF-8"))); // pk 2
                    
                    record.addColumn(new StringColumn("attr_1")); // cn
                    record.addColumn(new StringColumn("14922213432")); // ts
                    record.addColumn(new StringColumn("world")); // value
                    
                    p.input.add(record);
                    
                    p.expect.add(
                            new RecordAndMessage(
                                    record, 
                                    "The column of record is NULL, primary key name : pk_1 ."
                                    )
                            );
                }
                ps.add(p);
            }
        }
        
        {
            TestInvalidParam p = new TestInvalidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);
            p.pk.put("pk_1", PrimaryKeyType.INTEGER);
            p.pk.put("pk_2", PrimaryKeyType.BINARY);
            
            p.attr.put("attr_0", ColumnType.STRING);
            p.attr.put("attr_1", ColumnType.STRING);
            
            // Row
            {
                {
                    Record record = new DefaultRecord();
                    record.addColumn(new StringColumn("a")); // pk 0
                    record.addColumn(new LongColumn(1)); // pk 1
                    record.addColumn(new BytesColumn()); // pk 2
                    
                    record.addColumn(new StringColumn("attr_0")); // cn
                    record.addColumn(new StringColumn("14922213432")); // ts
                    record.addColumn(new StringColumn("hello")); // value
                    
                    p.input.add(record);
                    
                    p.expect.add(
                            new RecordAndMessage(
                                    record, 
                                    "The column of record is NULL, primary key name : pk_2 ."
                                    )
                            );
                }
                
                {
                    Record record = new DefaultRecord();
                    record.addColumn(new StringColumn("a")); // pk 0
                    record.addColumn(new LongColumn(1)); // pk 1
                    record.addColumn(new BytesColumn()); // pk 2
                    
                    record.addColumn(new StringColumn("attr_1")); // cn
                    record.addColumn(new StringColumn("14922213432")); // ts
                    record.addColumn(new StringColumn("world")); // value
                    
                    p.input.add(record);
                    
                    p.expect.add(
                            new RecordAndMessage(
                                    record, 
                                    "The column of record is NULL, primary key name : pk_2 ."
                                    )
                            );
                }
                ps.add(p);
            }
        }
        
        // check
        int index = 0;
        for (TestInvalidParam tp : ps) {
            LOG.info("TestParam Index : {}", index++);
            testIllegal(tp.pk, tp.attr, tp.input, tp.expect);
        }
    }
    
    @Test
    public void testForColumnName() throws Exception {
        
        // prepare
        List<TestInvalidParam> ps = new ArrayList<TestInvalidParam>();
        
        {
            TestInvalidParam p = new TestInvalidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);
            
            p.attr.put("attr_0", ColumnType.STRING);
            
            // Row
            {
                {
                    Record record = new DefaultRecord();
                    record.addColumn(new StringColumn("a")); // pk 0
                    
                    record.addColumn(new StringColumn()); // cn
                    record.addColumn(new StringColumn("14922213432")); // ts
                    record.addColumn(new StringColumn("hello")); // value
                    
                    p.input.add(record);
                    
                    p.expect.add(
                            new RecordAndMessage(
                                    record, 
                                    "The name of column should not be null or empty."
                                    )
                            );
                }
                ps.add(p);
            }
        }
        
        {
            TestInvalidParam p = new TestInvalidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);
            
            p.attr.put("", ColumnType.STRING);
            
            // Row
            {
                {
                    Record record = new DefaultRecord();
                    record.addColumn(new StringColumn("a")); // pk 0
                    
                    record.addColumn(new StringColumn("")); // cn
                    record.addColumn(new StringColumn("14922213432")); // ts
                    record.addColumn(new StringColumn("hello")); // value
                    
                    p.input.add(record);
                    
                    p.expect.add(
                            new RecordAndMessage(
                                    record, 
                                    "The name of column should not be null or empty."
                                    )
                            );
                }
                ps.add(p);
            }
        }
        
        {
            TestInvalidParam p = new TestInvalidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);
            
            p.attr.put("attr_0", ColumnType.STRING);
            
            // Row
            {
                {
                    Record record = new DefaultRecord();
                    record.addColumn(new StringColumn("a")); // pk 0
                    
                    record.addColumn(new BytesColumn()); // cn
                    record.addColumn(new StringColumn("14922213432")); // ts
                    record.addColumn(new StringColumn("hello")); // value
                    
                    p.input.add(record);
                    
                    p.expect.add(
                            new RecordAndMessage(
                                    record, 
                                    "The name of column should not be null or empty."
                                    )
                            );
                }
                ps.add(p);
            }
        }
        {
            TestInvalidParam p = new TestInvalidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);
            
            p.attr.put("attr_0", ColumnType.STRING);
            
            // Row
            {
                {
                    Record record = new DefaultRecord();
                    record.addColumn(new StringColumn("a")); // pk 0
                    
                    record.addColumn(new BoolColumn()); // cn
                    record.addColumn(new StringColumn("14922213432")); // ts
                    record.addColumn(new StringColumn("hello")); // value
                    
                    p.input.add(record);
                    
                    p.expect.add(
                            new RecordAndMessage(
                                    record, 
                                    "The name of column should not be null or empty."
                                    )
                            );
                }
                ps.add(p);
            }
        }
        {
            TestInvalidParam p = new TestInvalidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);
            
            p.attr.put("attr_0", ColumnType.STRING);
            
            // Row
            {
                {
                    Record record = new DefaultRecord();
                    record.addColumn(new StringColumn("a")); // pk 0
                    
                    record.addColumn(new DoubleColumn()); // cn
                    record.addColumn(new StringColumn("14922213432")); // ts
                    record.addColumn(new StringColumn("hello")); // value
                    
                    p.input.add(record);
                    
                    p.expect.add(
                            new RecordAndMessage(
                                    record, 
                                    "The name of column should not be null or empty."
                                    )
                            );
                }
                ps.add(p);
            }
        }
        // check
        int index = 0;
        for (TestInvalidParam tp : ps) {
            LOG.info("TestParam Index : {}", index++);
            testIllegal(tp.pk, tp.attr, tp.input, tp.expect);
        }
    }
    
    // TS为空
    // TS非法
    @Test
    public void testForTimestamp() throws Exception {
        List<TestInvalidParam> ps = new ArrayList<TestInvalidParam>();
        
        {
            TestInvalidParam p = new TestInvalidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);
            
            p.attr.put("attr_0", ColumnType.STRING);
            
            // Row
            {
                {
                    Record record = new DefaultRecord();
                    record.addColumn(new StringColumn("a")); // pk 0
                    
                    record.addColumn(new StringColumn("attr_0")); // cn
                    record.addColumn(new StringColumn()); // ts
                    record.addColumn(new StringColumn("hello")); // value
                    
                    p.input.add(record);
                    
                    p.expect.add(
                            new RecordAndMessage(
                                    record, 
                                    "The input timestamp can not be empty in the multiVersion mode."
                                    )
                            );
                }
                ps.add(p);
            }
        }
        
        {
            TestInvalidParam p = new TestInvalidParam();
            p.pk.put("pk_0", PrimaryKeyType.STRING);
            
            p.attr.put("attr_0", ColumnType.STRING);
            
            // Row
            {
                {
                    Record record = new DefaultRecord();
                    record.addColumn(new StringColumn("a")); // pk 0
                    
                    record.addColumn(new StringColumn("attr_0")); // cn
                    record.addColumn(new StringColumn("hello")); // ts
                    record.addColumn(new StringColumn("hello")); // value
                    
                    p.input.add(record);
                    
                    p.expect.add(
                            new RecordAndMessage(
                                    record, 
                                    "Code:[Common-01], Describe:[同步数据出现业务脏数据情况，数据类型转换错误 .] - String[\"hello\"]不能转为Long ."
                                    )
                            );
                }
                ps.add(p);
            }
        }
        // check
        int index = 0;
        for (TestInvalidParam tp : ps) {
            LOG.info("TestParam Index : {}", index++);
            testIllegal(tp.pk, tp.attr, tp.input, tp.expect);
        }
    }
}
