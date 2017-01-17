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
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSLine;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSMode;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSSendBuffer;
import com.alibaba.datax.plugin.writer.otswriter.utils.CollectorUtil;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.alibaba.datax.plugin.writer.otswriter.utils.ParseRecord;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.Row;

/**
 * 测试目的：主要是验证SendBuffer在各种Input组合下对Size的计算是否正确，场景如下：
 * 
 * 1.Input的行数
 *      1.1 1行
 *      1.2 100行
 *      1.3 150行
 * 2.是否有重复行
 *      2.1 否
 *      2.2 是
 * 3.操作类型
 *      3.1 PutRow
 *      3.2 UpdateRow
 */
public class OTSSendBufferUnittest {
    private static String tableName = "OTSSendBufferUnittest";
    public static Map<String, PrimaryKeyType> pk = new LinkedHashMap<String, PrimaryKeyType>();
    public static Map<String, ColumnType> attr = new LinkedHashMap<String, ColumnType>();
    
    @BeforeClass
    public static void init() {
        pk.put("pk_0", PrimaryKeyType.STRING);
        attr.put("attr_0", ColumnType.STRING);
    }
    
    @AfterClass
    public static void close() {}
    
    public static void test(
            Exception exception, 
            Map<PrimaryKey, Row> prepare, 
            OTSOpType type, 
            List<Record> input, 
            List<Row> expect, 
            List<Integer> rows) throws Exception {
        
        OTSConf conf = Conf.getConf(tableName, pk, attr, type);
        conf.setRetry(5);
        conf.setEncodePkColumnMapping(Utils.getPkColumnMapping(conf.getPrimaryKeyColumn()));
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        MockOTSClient ots = new MockOTSClient(10000, exception, prepare);
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
        
        assertEquals(0, collector.getContent().size());
        assertEquals(true, DataChecker.checkRow(OTSHelper.getAllData(ots, conf), expect, false));
        assertEquals(true, DataChecker.checkRowsCountPerRequest( ots.getRowsCountPerRequest(), rows)); 
    }
    
    public static void testForMultiVersion(
            Exception exception, 
            Map<PrimaryKey, Row> prepare, 
            OTSOpType type, 
            List<List<Record>> input, 
            List<Row> expect, 
            List<Integer> rows) throws Exception {
        
        OTSConf conf = Conf.getConf(tableName, pk, attr, type);
        conf.setMode(OTSMode.MULTI_VERSION);
        conf.getAttributeColumn().add(new OTSAttrColumn("attr_1", "attr_1", ColumnType.STRING));
        conf.setRetry(5);
        conf.setEncodePkColumnMapping(Utils.getPkColumnMapping(conf.getPrimaryKeyColumn()));
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        MockOTSClient ots = new MockOTSClient(5000, exception, prepare);
        CollectorUtil.init(collector);
        OTSSendBuffer buffer = new OTSSendBuffer(ots, conf);
        
        for (List<Record> en : input) {
            OTSLine line = ParseRecord.parseMultiVersionRecordToOTSLine(
                    conf.getTableName(), 
                    conf.getOperation(), 
                    Common.getPkColumnMapping(conf.getEncodePkColumnMapping()), 
                    null, 
                    Common.getPKFromRecord(Common.getPkColumnMapping(conf.getEncodePkColumnMapping()), en.get(0)),
                    en);
            buffer.write(line);
        }
        buffer.close();
        
        assertEquals(0, collector.getContent().size());
        assertEquals(true, DataChecker.checkRow(OTSHelper.getAllData(ots, conf), expect, true));
        assertEquals(true, DataChecker.checkRowsCountPerRequest(rows, ots.getRowsCountPerRequest())); 
    }
    
    public static void testIllegal(Exception exception, Map<PrimaryKey, Row> prepare, OTSOpType type, List<Record> input, List<Record> expect, List<Integer> rows) throws Exception {
        OTSConf conf = Conf.getConf(tableName, pk, attr, type);
        conf.setRetry(5);
        conf.setEncodePkColumnMapping(Utils.getPkColumnMapping(conf.getPrimaryKeyColumn()));
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        MockOTSClient ots = new MockOTSClient(5000, exception, prepare);
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
        
        assertEquals(expect.size(), collector.getContent().size());
        assertEquals(true, DataChecker.checkRecord(collector.getRecord(), expect)); 
        assertEquals(true, DataChecker.checkRowsCountPerRequest(ots.getRowsCountPerRequest(), rows)); 
    }

    // 1行
    // 输入：1行数据，采用PutRow的方式，期望：该行数据被成功写入，且只调用1次BatchWriteRow接口，每次调用API写入的行数为1 （覆盖场景：1.1、3.1， 测试正常逻辑是否符合预期）
    @Test
    public void testCase1() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello"));
            r.addColumn(new StringColumn());
            input.add(r);
        }
        test(null, null, OTSOpType.PUT_ROW, input, expect, rowsExpect);
    }
    
    // 输入：1行数据，采用UpdateRow的方式，期望：该行数据被成功写入，且只调用1次BatchWriteRow接口，每次调用API写入的行数为1（覆盖场景：1.1、3.2，测试正常逻辑是否符合预期）
    @Test
    public void testCase2() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello"));
            r.addColumn(new StringColumn());
            input.add(r);
        }
        rowsExpect.add(1);
        test(null, null, OTSOpType.UPDATE_ROW, input, expect, rowsExpect);
    }
    
    // 输入：1行数据，该行的STRING PK的长度为1025B，期望：该行数据写入失败，且只调用1次BatchWriteRow接口，每次调用API写入的行数为1（覆盖场景：1.1，测试单行参数错误是否符合预期）
    @Test
    public void testCase3() throws Exception {
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0 ; i < 1025; i++) {
            sb.append("a");
        }
        
        List<Record> input = new ArrayList<Record>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn(sb.toString()));
            r.addColumn(new StringColumn("world"));
            input.add(r);
        }
        rowsExpect.add(1);
        testIllegal(null, null, OTSOpType.PUT_ROW, input, new ArrayList<Record>(input), rowsExpect);
    }
    
    
    // 输入：1行数据，该行的STRING ATTR的长度为64KB + 1B，期望：该行数据写入失败，且只调用1次BatchWriteRow接口，每次调用API写入的行数为1（覆盖场景：1.1，测试单行参数错误是否符合预期）
    @Test
    public void testCase4() throws Exception {
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0 ; i < (64*1024 + 1); i++) {
            sb.append("a");
        }
        
        List<Record> input = new ArrayList<Record>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("world"));
            r.addColumn(new StringColumn(sb.toString()));
            input.add(r);
        }
        rowsExpect.add(1);
        testIllegal(null, null, OTSOpType.PUT_ROW, input, new ArrayList<Record>(input), rowsExpect);
    }
    
    // 100行数据
    // 输入：100行数据，采用PutRow的方式，无重复数据，期望：该行数据被成功写入，且只调用1次BatchWriteRow接口，每次调用API写入的行数为100（覆盖场景：1.2、3.1、2.1，测试多行数据正常逻辑是否符合预期）
    @Test
    public void testCase5() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        for (int i = 0; i < 100; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            
            Row row =  OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello_" + i))
                    .addAttrColumn("attr_0", ColumnValue.fromString("world_" + i), 1)
                    .toRow();

            expect.add(row);
        }
        rowsExpect.add(100);
        test(null, null, OTSOpType.PUT_ROW, input, expect, rowsExpect);
    }
    
    // 输入：100 + 1行数据，采用PutRow的方式，有一行重复数据，期望：该行数据被成功写入，
    // 且只调用1次BatchWriteRow接口，每次调用API写入的行数为100（覆盖场景：1.2、3.1、2.2，测试多行数据中又重复的行的计算逻辑是否符合预期）
    @Test
    public void testCase6() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        // 重复行
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + 0));
            r.addColumn(new StringColumn("world_" + 0));
            input.add(r);
        }
        
        for (int i = 0; i < 100; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            
            Row row =  OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello_" + i))
                    .addAttrColumn("attr_0", ColumnValue.fromString("world_" + i), 1)
                    .toRow();
            expect.add(row);
        }
        rowsExpect.add(100);
        rowsExpect.add(1);
        test(null, null, OTSOpType.PUT_ROW, input, expect, rowsExpect);
    }
    // 输入：100行数据，采用UpdateRow的方式，无重复数据，期望：该行数据被成功写入，且只调用1次BatchWriteRow接口，每次调用API写入的行数为100（覆盖场景：1.2、3.2、2.1， 测试多行数据正常逻辑是否符合预期）
    @Test
    public void testCase7() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        for (int i = 0; i < 100; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            
            Row row =  OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello_" + i))
                    .addAttrColumn("attr_0", ColumnValue.fromString("world_" + i), 1)
                    .toRow();
            expect.add(row);
        }
        rowsExpect.add(100);
        test(null, null, OTSOpType.UPDATE_ROW, input, expect, rowsExpect);
    }
    // 输入：100 + 1行数据，采用UpdateRow的方式，有一行重复数据，期望：该行数据被成功写入，且只调用1次BatchWriteRow接口，每次调用API写入的行数为100（覅该场景：1.2、3.2、2.2，测试多行数据中又重复的行的计算逻辑是否符合预期）
    @Test
    public void testCase8() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        // 重复行
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + 0));
            r.addColumn(new StringColumn("world_" + 0));
            input.add(r);
        }
        
        for (int i = 0; i < 100; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            Row row =  OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello_99"))
                    .addAttrColumn("attr_0", ColumnValue.fromString("world_99"), 1)
                    .toRow();
            expect.add(row);
        }
        rowsExpect.add(100);
        rowsExpect.add(1);
        test(null, null, OTSOpType.UPDATE_ROW, input, expect, rowsExpect);
    }
    // 150行
    // 输入：150行数据，采用PutRow的方式，无重复数据，期望：该行数据被成功写入，且只调用2次BatchWriteRow接口，每次调用API写入的行数为100、50 （覆盖场景：1.3，3.1、2.1，测试多行数据正常逻辑是否符合预期）
    @Test
    public void testCase9() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        for (int i = 0; i < 150; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            
            Row row =  OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello_" + i))
                    .addAttrColumn("attr_0", ColumnValue.fromString("world_" + i), 1)
                    .toRow();
            expect.add(row);
        }
        rowsExpect.add(100);
        rowsExpect.add(50);
        test(null, null, OTSOpType.PUT_ROW, input, expect, rowsExpect);
    }
    // 输入：150行数据，采用PutRow的方式，51行重复数据，期望：该行数据被成功写入，且只调用2次BatchWriteRow接口，每次调用API写入的行数为100、50 （覆盖场景：1.3，3.1、2.2，测试多行数据中又重复的行的计算逻辑是否符合预期）
    @Test
    public void testCase10() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        // 重复行
        for (int i = 0; i < 51; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
        }
        
        for (int i = 0; i < 150; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            
            Row row =  OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello_" + i))
                    .addAttrColumn("attr_0", ColumnValue.fromString("world_" + i), 1)
                    .toRow();
            expect.add(row);
        }
        rowsExpect.add(100);
        rowsExpect.add(100);
        rowsExpect.add(1);
        test(null, null, OTSOpType.PUT_ROW, input, expect, rowsExpect);
    }
    
    // 输入：150行数据，采用UpdateRow的方式，无重复数据，期望：该行数据被成功写入，且只调用2次BatchWriteRow接口，每次调用API写入的行数为100、50 （覆盖场景：1.3，3.2、2.1，测试多行数据正常逻辑是否符合预期）
    @Test
    public void testCase11() throws Exception {
        
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        for (int i = 0; i < 150; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            
            Row row =  OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello_" + i))
                    .addAttrColumn("attr_0", ColumnValue.fromString("world_" + i), 1)
                    .toRow();
            expect.add(row);
        }
        rowsExpect.add(100);
        rowsExpect.add(50);
        test(null, null, OTSOpType.UPDATE_ROW, input, expect, rowsExpect);
    }
    
    // 输入：150行数据，采用UpdateRow的方式，51行重复数据，期望：该行数据被成功写入，且只调用2次BatchWriteRow接口，每次调用API写入的行数为100、50 （覆盖场景：1.3，3.2、2.2，测试多行数据中又重复的行的计算逻辑是否符合预期）
    @Test
    public void testCase12() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        // 重复行
        for (int i = 0; i < 50; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
        }
        
        for (int i = 0; i < 150; i++) {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("hello_" + i));
            r.addColumn(new StringColumn("world_" + i));
            input.add(r);
            
            Row row =  OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello_" + i))
                    .addAttrColumn("attr_0", ColumnValue.fromString("world_" + i), 1)
                    .toRow();
            expect.add(row);
        }
        rowsExpect.add(100);
        rowsExpect.add(100);
        test(null, null, OTSOpType.UPDATE_ROW, input, expect, rowsExpect);
    }
    
    // 在Put模式下测试多版本，2行数据，每个Column都有100个版本。期望：数据被成功写入，调用一次BatchWriteRow接口
    @Test
    public void testCase13() throws Exception {
        List<List<Record>> input = new ArrayList<List<Record>>();
        List<Row> expect = new ArrayList<Row>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        {
            {
                // 第一行数据
                // 50个版本
                List<Record> line = new ArrayList<Record>();
                for (int i = 1; i < 51; i++) {
                    Record r = new DefaultRecord();
                    r.addColumn(new StringColumn("hello"));
                    r.addColumn(new StringColumn("attr_0"));
                    r.addColumn(new LongColumn(i));
                    r.addColumn(new StringColumn("value"));
                    line.add(r);
                }
                input.add(line);
            }
            
            {
                // 第二行数据
                // 50个版本
                List<Record> line = new ArrayList<Record>();
                for (int i = 1; i < 51; i++) {
                    Record r = new DefaultRecord();
                    r.addColumn(new StringColumn("world"));
                    r.addColumn(new StringColumn("attr_0"));
                    r.addColumn(new LongColumn(i));
                    r.addColumn(new LongColumn(100));
                    line.add(r);
                }
                input.add(line);
            }
            
            // 期望
            OTSRowBuilder helper0 =  OTSRowBuilder.newInstance();
            helper0.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello"));
            
            OTSRowBuilder helper1 =  OTSRowBuilder.newInstance();
            helper1.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("world"));
            for (int i = 1; i < 51; i++) {
                helper0.addAttrColumn("attr_0", ColumnValue.fromString("value"), i);
                helper1.addAttrColumn("attr_0", ColumnValue.fromLong(100), i);
            }
            
            expect.add(helper0.toRow());
            expect.add(helper1.toRow());
            
            rowsExpect.add(2); // 两行数据
        }
        
        testForMultiVersion(null, null, OTSOpType.UPDATE_ROW, input, expect, rowsExpect);
    }
    
    // 在Update模式下测试多版本，2行数据，每个Column都有100个版本。期望：数据被成功写入，调用一次BatchWriteRow接口
    @Test
    public void testCase14() throws Exception {
        List<List<Record>> input = new ArrayList<List<Record>>();
        Map<PrimaryKey, Row> prepare = new LinkedHashMap<PrimaryKey, Row>();
        List<Row> expect = new ArrayList<Row>();
        List<Integer> rowsExpect = new ArrayList<Integer>(); // 每轮切分的行数
        
        {
            // 环境准备
            Row row =  OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello"))
                    .addAttrColumn("attr_1", ColumnValue.fromString("value"), 100)
                    .toRow();
            prepare.put(row.getPrimaryKey(), row);
            {
                 // 第一行数据
                // 50个版本
                List<Record> line = new ArrayList<Record>();
                for (int i = 1; i < 51; i++) {
                    Record r = new DefaultRecord();
                    r.addColumn(new StringColumn("hello"));
                    r.addColumn(new StringColumn("attr_0"));
                    r.addColumn(new LongColumn(i));
                    r.addColumn(new StringColumn("value"));
                    line.add(r);
                }
                input.add(line);
            }
            
            {
                // 第二行数据
                // 50个版本
                List<Record> line = new ArrayList<Record>();
                for (int i = 1; i < 51; i++) {
                    Record r = new DefaultRecord();
                    r.addColumn(new StringColumn("world"));
                    r.addColumn(new StringColumn("attr_0"));
                    r.addColumn(new LongColumn(i));
                    r.addColumn(new LongColumn(100));
                    line.add(r);
                }
                input.add(line);
            }
            
            // 期望
            OTSRowBuilder helper0 =  OTSRowBuilder.newInstance();
            helper0.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("hello"));
            helper0.addAttrColumn("attr_1", ColumnValue.fromString("value"), 100); // 原表已存在
            
            OTSRowBuilder helper1 =  OTSRowBuilder.newInstance();
            helper1.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("world"));
            for (int i = 1; i < 51; i++) {
                helper0.addAttrColumn("attr_0", ColumnValue.fromString("value"), i);
                helper1.addAttrColumn("attr_0", ColumnValue.fromLong(100), i);
            }
            
            expect.add(helper0.toRow());
            expect.add(helper1.toRow());
            
            rowsExpect.add(2); // 两行数据
        }
        
        testForMultiVersion(null, prepare, OTSOpType.UPDATE_ROW, input, expect, rowsExpect);
    }
}
