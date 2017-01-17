package com.alibaba.datax.plugin.writer.otswriter.unittest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.openservices.ots.internal.MockOTSClient;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.otswriter.OTSCriticalException;
import com.alibaba.datax.plugin.writer.otswriter.OtsWriterSlaveProxy;
import com.alibaba.datax.plugin.writer.otswriter.OtsWriterSlaveProxyNormal;
import com.alibaba.datax.plugin.writer.otswriter.common.Conf;
import com.alibaba.datax.plugin.writer.otswriter.common.DataChecker;
import com.alibaba.datax.plugin.writer.otswriter.common.OTSHelper;
import com.alibaba.datax.plugin.writer.otswriter.common.OTSRowBuilder;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.common.Utils;
import com.alibaba.datax.plugin.writer.otswriter.common.TestPluginCollector.RecordAndMessage;
import com.alibaba.datax.plugin.writer.otswriter.common.TestRecordReceiver;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.Row;

/**
 * 测试目的：测试在各种模式组合下的解析行为
 * 
 * * 普通模式的处理
 * ** 正常
 * ** pk有部分列为空的情况
 * ** attr有部分列为空的情况
 * ** 指定ts的情况
 * ** 不指定ts的情况
 * ** pk列部分不能转换
 * ** attr列部分列不能转换
 * 
 * @author redchen
 *
 */
public class OtsWriterSlaveProxyNormalUnittest {
    
    private String tableName = "OtsWriterSlaveProxyNormalUnittest";
    private OtsWriterSlaveProxy proxy = null;
    private SyncClientInterface ots = null;
    
    @BeforeClass
    public static void beforeClass() {}
    
    @Before
    public void setup() {
        ots = new MockOTSClient(5000, null, null);
    }
    
    @After
    public void teardown() {
        ots.shutdown();
    }
    
    private void test(OTSConf conf, List<Record> contents, List<Row> expect) throws OTSCriticalException {
        conf.setEncodePkColumnMapping(Utils.getPkColumnMapping(conf.getPrimaryKeyColumn()));
        RecordReceiver lineReceiver = new TestRecordReceiver(contents);
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        proxy = new OtsWriterSlaveProxyNormal(ots, conf);
        proxy.init(collector);
        proxy.write(lineReceiver);
        proxy.close();
        
        assertEquals(0, collector.getContent().size());
        assertEquals(true, DataChecker.checkRow(OTSHelper.getAllData(ots, conf), expect, false));
    }
    
    private void testWithTs(OTSConf conf, List<Record> contents, List<Row> expect) throws OTSCriticalException {
        conf.setEncodePkColumnMapping(Utils.getPkColumnMapping(conf.getPrimaryKeyColumn()));
        RecordReceiver lineReceiver = new TestRecordReceiver(contents);
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        proxy = new OtsWriterSlaveProxyNormal(ots, conf);
        proxy.init(collector);
        proxy.write(lineReceiver);
        proxy.close();
        
        assertEquals(0, collector.getContent().size());
        assertEquals(true, DataChecker.checkRow(OTSHelper.getAllData(ots, conf), expect, true));
    }
    
    private void testIllegel(OTSConf conf, List<Record> contents, List<RecordAndMessage> expect) throws OTSCriticalException {
        conf.setEncodePkColumnMapping(Utils.getPkColumnMapping(conf.getPrimaryKeyColumn()));
        RecordReceiver lineReceiver = new TestRecordReceiver(contents);
        Configuration configuration = Configuration.newDefault();
        TestPluginCollector collector = new TestPluginCollector(configuration, null, null);
        proxy = new OtsWriterSlaveProxyNormal(ots, conf);
        proxy.init(collector);
        proxy.write(lineReceiver);
        proxy.close();
        
        assertEquals(expect.size(), collector.getContent().size());
        assertEquals(true, DataChecker.checkRecordWithMessage(collector.getContent(), expect));
    }
    
    /**
     * * 普通模式的处理
     * ** 正常
     * @throws OTSCriticalException
     */
    @Test
    public void test_pk_valid() throws OTSCriticalException {
        Map<String, PrimaryKeyType> pk = new LinkedHashMap<String, PrimaryKeyType>();
        Map<String, ColumnType> attr = new LinkedHashMap<String, ColumnType>(); 
        
        pk.put("pk_0", PrimaryKeyType.STRING);
        attr.put("attr_0", ColumnType.STRING);
        
        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.UPDATE_ROW);
        
        //
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("big"));
            r.addColumn(new StringColumn("yes"));
            input.add(r);
            
            Row row =  OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("big"))
                    .addAttrColumn("attr_0", ColumnValue.fromString("yes"), 1)
                    .toRow();
            expect.add(row);
        }
        
        test(conf, input, expect);
    }
    
    /**
     * * 普通模式的处理
     * ** pk有部分列为空的情况
     * @throws OTSCriticalException
     */
    @Test
    public void test_pk_null_column() throws OTSCriticalException {
        Map<String, PrimaryKeyType> pk = new LinkedHashMap<String, PrimaryKeyType>();
        Map<String, ColumnType> attr = new LinkedHashMap<String, ColumnType>(); 
        
        pk.put("pk_0", PrimaryKeyType.STRING);
        attr.put("attr_0", ColumnType.STRING);
        
        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.UPDATE_ROW);
        
        //
        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn());
            r.addColumn(new StringColumn("yes"));
            input.add(r);
            
            expect.add(new RecordAndMessage(r, "The column of record is NULL, primary key name : pk_0 ."));
        }
        testIllegel(conf, input, expect);
    }
    
    /**
     * * 普通模式的处理
     * ** attr有部分列为空的情况
     * @throws OTSCriticalException
     */
    @Test
    public void test_attr_null_column() throws OTSCriticalException {
        Map<String, PrimaryKeyType> pk = new LinkedHashMap<String, PrimaryKeyType>();
        Map<String, ColumnType> attr = new LinkedHashMap<String, ColumnType>(); 
        
        pk.put("pk_0", PrimaryKeyType.STRING);
        attr.put("attr_0", ColumnType.STRING);
        attr.put("attr_1", ColumnType.INTEGER);
        
        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.UPDATE_ROW);
        
        //
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("big"));
            r.addColumn(new StringColumn("yes"));
            r.addColumn(new LongColumn());
            input.add(r);
            
            Row row =  OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("big"))
                    .addAttrColumn("attr_0", ColumnValue.fromString("yes"), 1)
                    .toRow();
            expect.add(row);
        }
        
        test(conf, input, expect);
    }
    
    /**
     * * 普通模式的处理
     * ** 指定ts的情况
     * @throws OTSCriticalException
     */
    @Test
    public void test_specify_ts() throws OTSCriticalException {
        Map<String, PrimaryKeyType> pk = new LinkedHashMap<String, PrimaryKeyType>();
        Map<String, ColumnType> attr = new LinkedHashMap<String, ColumnType>(); 
        
        pk.put("pk_0", PrimaryKeyType.STRING);
        attr.put("attr_0", ColumnType.STRING);
        attr.put("attr_1", ColumnType.INTEGER);
        
        long timestamp = System.currentTimeMillis();
        
        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.UPDATE_ROW);
        conf.setTimestamp(timestamp);
        //
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("big"));
            r.addColumn(new StringColumn("yes"));
            r.addColumn(new LongColumn());
            input.add(r);
            
            Row row =  OTSRowBuilder.newInstance()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString("big"))
                    .addAttrColumn("attr_0", ColumnValue.fromString("yes"), timestamp)
                    .toRow();
            expect.add(row);
        }
        
        testWithTs(conf, input, expect);
    }
    
    /**
     * * 普通模式的处理
     * ** pk列部分不能转换
     * @throws OTSCriticalException
     */
    @Test
    public void test_pk_column_conversion_fail() throws OTSCriticalException {
        Map<String, PrimaryKeyType> pk = new LinkedHashMap<String, PrimaryKeyType>();
        Map<String, ColumnType> attr = new LinkedHashMap<String, ColumnType>(); 
        
        pk.put("pk_0", PrimaryKeyType.INTEGER);
        attr.put("attr_0", ColumnType.STRING);
        
        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.UPDATE_ROW);
        
        //
        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("big"));
            r.addColumn(new StringColumn("yes"));
            input.add(r);
            
            expect.add(new RecordAndMessage(r, "Column coversion error, src type : STRING, src value: big, expect type: INTEGER ."));
        }
        testIllegel(conf, input, expect);
    }
    
    /**
     * * 普通模式的处理
     * ** pk列部分不能转换
     * @throws OTSCriticalException
     */
    @Test
    public void test_attr_column_conversion_fail() throws OTSCriticalException {
        Map<String, PrimaryKeyType> pk = new LinkedHashMap<String, PrimaryKeyType>();
        Map<String, ColumnType> attr = new LinkedHashMap<String, ColumnType>(); 
        
        pk.put("pk_0", PrimaryKeyType.STRING);
        attr.put("attr_0", ColumnType.INTEGER);
        
        OTSConf conf = Conf.getConf(tableName, pk, attr, OTSOpType.UPDATE_ROW);
        
        //
        List<Record> input = new ArrayList<Record>();
        List<RecordAndMessage> expect = new ArrayList<RecordAndMessage>();
        {
            Record r = new DefaultRecord();
            r.addColumn(new StringColumn("big"));
            r.addColumn(new StringColumn("yes"));
            input.add(r);
            
            expect.add(new RecordAndMessage(r, "Column coversion error, src type : STRING, src value: yes, expect type: INTEGER ."));
        }
        testIllegel(conf, input, expect);
    }
}
