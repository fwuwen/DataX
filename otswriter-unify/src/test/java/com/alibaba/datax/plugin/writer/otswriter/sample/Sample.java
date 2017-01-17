package com.alibaba.datax.plugin.writer.otswriter.sample;

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
public class Sample extends BaseTest{
    
    private static String tableName = "AATest";
    
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
        tableMeta.addPrimaryKeyColumn("userid", PrimaryKeyType.STRING);

        //OTSHelper.createTableSafe(ots, tableMeta);
    }
    
    @After
    public void teardown() {}
    
    // PK的String Column的值等于1KB
    @Test
    public void test() throws Exception {
        Map<String, ColumnType> attr = new LinkedHashMap<String, ColumnType>();
        attr.put("cf0_string_column", ColumnType.STRING);
        attr.put("cf0_int_column", ColumnType.INTEGER);
        attr.put("cf0_binary_column", ColumnType.BINARY);
        attr.put("cf0_boolean_column", ColumnType.BOOLEAN);
        attr.put("cf0_double_column", ColumnType.DOUBLE);
        attr.put("cf1_string_column", ColumnType.STRING);
        attr.put("cf1_int_column", ColumnType.INTEGER);
        attr.put("cf1_binary_column", ColumnType.BINARY);
        attr.put("cf1_boolean_column", ColumnType.BOOLEAN);
        attr.put("cf1_double_column", ColumnType.DOUBLE);

        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                tableMeta.getPrimaryKeyMap(), 
                attr,
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        List<Row> rows = OTSHelper.getAllData(ots, conf);
        for (Row r : rows) {
            System.out.println(r);
        }
    }
}
