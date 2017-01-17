package com.alibaba.datax.plugin.writer.otswriter.functiontest;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.TableMeta;

public class MultiVersionMixPKFunctiontest extends BaseTest{
    private static String tableName = "MultiVersionMixPKFunctiontest";
    private static SyncClientInterface ots = Utils.getOTSClient();
    
    @BeforeClass
    public static void setBeforeClass() {}
    
    @AfterClass
    public static void setAfterClass() {
        ots.shutdown();
    }
    
    @Before
    public void setup() throws Exception {}
    
    @After
    public void teardown() {}
    
    public void testMixPK(TableMeta meta) throws Exception {
        OTSHelper.createTableSafe(ots, meta);
        
        List<PrimaryKeySchema> pks = meta.getPrimaryKeyList();
        int pkCount = pks.size();
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        // 构造数据
        
        {
            long ts = System.currentTimeMillis();
            
            for (int rowIndex = 100; rowIndex < 200; rowIndex++) {
                OTSRowBuilder row = OTSRowBuilder.newInstance();
                // pk
                for (int pkCountIndex = 0; pkCountIndex < pkCount; pkCountIndex++) {
                    row.addPrimaryKeyColumn(getPKColumnName(pkCountIndex), Utils.getPKColumnValue(pks.get(pkCountIndex).getType(), rowIndex));
                }

                for (int i = 0; i < 2; i++) {
                    String columnName = getColumnName(i);
                    for (int j = 0; j < 2; j++) {
                        Record r = new DefaultRecord();
                        // pk
                        for (int pkCountIndex = 0; pkCountIndex < pkCount; pkCountIndex++) {
                            r.addColumn(Utils.getPKColumn(pks.get(pkCountIndex).getType(), rowIndex));
                        }
                        // columnName
                        r.addColumn(new StringColumn(columnName));
                        // timestamp
                        r.addColumn(new LongColumn(ts + j));
                        // value
                        r.addColumn(new LongColumn(j));
                        input.add(r);
                        
                        row.addAttrColumn(columnName, ColumnValue.fromLong(j), ts + j);
                    }
                }
                expect.add(row.toRow());
            }
        }
        
        // check
        OTSConf conf = Conf.getConf(
                tableName, 
                meta.getPrimaryKeyMap(), 
                getColumnMeta(2, ColumnType.INTEGER), 
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        testWithTS(ots, conf, input, expect);
    }
    
    /**
     *  测试目的：测试在多PK和不同类型的组合下程序是否符合预期。
     *  测试内容，分别构造PK列为
     *  （1列PK）(string)、(integer)、(binary)、
     *  （2列PK）(string、integer)、（integer、binary）、（binary、string），
     *  （4列PK）（string，string，integer，binary）、（integer、string、binary，string）、（binary、string，integer、integer），
     *  期望数据符合预期。
     * @throws Exception 
     */
    @Test
    public void testCase() throws Exception {
        List<TableMeta> tableMetas = new ArrayList<TableMeta>();
        {
            TableMeta meta = new TableMeta(tableName);
            meta.addPrimaryKeyColumn(getPKColumnName(0), PrimaryKeyType.STRING);
            tableMetas.add(meta);
        }
        {
            TableMeta meta = new TableMeta(tableName);
            meta.addPrimaryKeyColumn(getPKColumnName(0), PrimaryKeyType.INTEGER);
            tableMetas.add(meta);
        }
        {
            TableMeta meta = new TableMeta(tableName);
            meta.addPrimaryKeyColumn(getPKColumnName(0), PrimaryKeyType.BINARY);
            tableMetas.add(meta);
        }
        {
            TableMeta meta = new TableMeta(tableName);
            meta.addPrimaryKeyColumn(getPKColumnName(0), PrimaryKeyType.STRING);
            meta.addPrimaryKeyColumn(getPKColumnName(1), PrimaryKeyType.INTEGER);
            tableMetas.add(meta);
        }
        {
            TableMeta meta = new TableMeta(tableName);
            meta.addPrimaryKeyColumn(getPKColumnName(0), PrimaryKeyType.INTEGER);
            meta.addPrimaryKeyColumn(getPKColumnName(1), PrimaryKeyType.BINARY);
            tableMetas.add(meta);
        }
        {
            TableMeta meta = new TableMeta(tableName);
            meta.addPrimaryKeyColumn(getPKColumnName(0), PrimaryKeyType.BINARY);
            meta.addPrimaryKeyColumn(getPKColumnName(1), PrimaryKeyType.STRING);
            tableMetas.add(meta);
        }
        {
            TableMeta meta = new TableMeta(tableName);
            meta.addPrimaryKeyColumn(getPKColumnName(0), PrimaryKeyType.STRING);
            meta.addPrimaryKeyColumn(getPKColumnName(1), PrimaryKeyType.INTEGER);
            meta.addPrimaryKeyColumn(getPKColumnName(2), PrimaryKeyType.INTEGER);
            meta.addPrimaryKeyColumn(getPKColumnName(3), PrimaryKeyType.INTEGER);
            tableMetas.add(meta);
        }
        {
            TableMeta meta = new TableMeta(tableName);
            meta.addPrimaryKeyColumn(getPKColumnName(0), PrimaryKeyType.STRING);
            meta.addPrimaryKeyColumn(getPKColumnName(1), PrimaryKeyType.INTEGER);
            meta.addPrimaryKeyColumn(getPKColumnName(2), PrimaryKeyType.INTEGER);
            meta.addPrimaryKeyColumn(getPKColumnName(3), PrimaryKeyType.INTEGER);
            tableMetas.add(meta);
        }
        {
            TableMeta meta = new TableMeta(tableName);
            meta.addPrimaryKeyColumn(getPKColumnName(0), PrimaryKeyType.STRING);
            meta.addPrimaryKeyColumn(getPKColumnName(1), PrimaryKeyType.INTEGER);
            meta.addPrimaryKeyColumn(getPKColumnName(2), PrimaryKeyType.INTEGER);
            meta.addPrimaryKeyColumn(getPKColumnName(3), PrimaryKeyType.INTEGER);
            tableMetas.add(meta);
        }
        
        for (TableMeta meta : tableMetas) {
            testMixPK(meta);
        }
    }
}
