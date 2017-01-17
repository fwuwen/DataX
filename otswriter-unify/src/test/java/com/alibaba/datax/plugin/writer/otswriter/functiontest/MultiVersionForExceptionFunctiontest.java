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

public class MultiVersionForExceptionFunctiontest extends BaseTest {
    private static String tableName = "MultiVersionForExceptionFunctiontest";
    private static SyncClientInterface ots = Utils.getOTSClient();
    private static TableMeta tableMeta = null;

    @BeforeClass
    public static void setBeforeClass() {
    }

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
    public void teardown() {
    }

    /**
     * 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。
     * 测试内容：用户配置3列PK，构造10个Cell，其中一个Cell只传入2列PK，期望writer异常退出，错误消息符合预期
     *
     * @throws Exception
     */
    @Test
    public void testCase1() throws Exception {
        List<Record> input = new ArrayList<Record>();
        // 构造数据
        {
            long ts = 1423572444981L;

            String columnName = getColumnName(0);
            for (int j = 0; j < 9; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));

                // columnName
                r.addColumn(new StringColumn(columnName));
                // timestamp
                r.addColumn(new LongColumn(ts + j));
                // value
                r.addColumn(new LongColumn(j));
                input.add(r);
            }
            for (int j = 9; j < 10; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                // 少一个PK

                // columnName
                r.addColumn(new StringColumn(columnName));
                // timestamp
                r.addColumn(new LongColumn(ts + j));
                // value
                r.addColumn(new LongColumn(j));
                input.add(r);
            }
        }

        // check
        OTSConf conf = Conf.getConf(
                tableName,
                tableMeta.getPrimaryKeyMap(),
                getColumnMeta(1, ColumnType.INTEGER),
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        test(ots, conf, input, "Size of record not equal size of config column. record size : 5, config column size : 6, record data : {\"data\":[{\"byteSize\":9,\"rawData\":\"Uid_value\",\"type\":\"STRING\"},{\"byteSize\":8,\"rawData\":1,\"type\":\"LONG\"},{\"byteSize\":11,\"rawData\":\"attr_000000\",\"type\":\"STRING\"},{\"byteSize\":8,\"rawData\":1423572444990,\"type\":\"LONG\"},{\"byteSize\":8,\"rawData\":9,\"type\":\"LONG\"}],\"size\":5}.");
    }

    /**
     * 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。
     * 测试内容：用户配置3列PK，构造10个Cell，其中一个Cell只传入4列PK，期望writer异常退出，错误消息符合预期
     *
     * @throws Exception
     */
    @Test
    public void testCase2() throws Exception {
        List<Record> input = new ArrayList<Record>();
        // 构造数据
        {
            long ts = 1423572444981L;

            String columnName = getColumnName(0);
            for (int j = 0; j < 9; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));

                // columnName
                r.addColumn(new StringColumn(columnName));
                // timestamp
                r.addColumn(new LongColumn(ts + j));
                // value
                r.addColumn(new LongColumn(j));
                input.add(r);
            }
            for (int j = 9; j < 10; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));// 多一个PK

                // columnName
                r.addColumn(new StringColumn(columnName));
                // timestamp
                r.addColumn(new LongColumn(ts + j));
                // value
                r.addColumn(new LongColumn(j));
                input.add(r);
            }
        }

        // check
        OTSConf conf = Conf.getConf(
                tableName,
                tableMeta.getPrimaryKeyMap(),
                getColumnMeta(1, ColumnType.INTEGER),
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        test(ots, conf, input, "Size of record not equal size of config column. record size : 7, config column size : 6, record data : {\"data\":[{\"byteSize\":9,\"rawData\":\"Uid_value\",\"type\":\"STRING\"},{\"byteSize\":8,\"rawData\":1,\"type\":\"LONG\"},{\"byteSize\":9,\"rawData\":\"TWlkX3ZhbHVl\",\"type\":\"BYTES\"},{\"byteSize\":9,\"rawData\":\"TWlkX3ZhbHVl\",\"type\":\"BYTES\"},{\"byteSize\":11,\"rawData\":\"attr_000000\",\"type\":\"STRING\"},{\"byteSize\":8,\"rawData\":1423572444990,\"type\":\"LONG\"},{\"byteSize\":8,\"rawData\":9,\"type\":\"LONG\"}],\"size\":7}.");
    }

    /**
     * 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。
     * 测试内容：用户配置3列PK，构造10个Cell，其中一个Cell只传入3列PK，但是有一个PK列为空，期望该Cell被记录到脏数据回收器中，错误消息符合预期
     *
     * @throws Exception
     */
    @Test
    public void testCase3() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<RecordAndMessage> rm = new ArrayList<RecordAndMessage>();
        // 构造数据
        {
            long ts = System.currentTimeMillis();

            OTSRowBuilder row = OTSRowBuilder.newInstance();
            row.addPrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("Uid_value"));
            row.addPrimaryKeyColumn("Pid", PrimaryKeyValue.fromLong(1));
            row.addPrimaryKeyColumn("Mid", PrimaryKeyValue.fromBinary("Mid_value".getBytes()));

            String columnName = getColumnName(0);
            for (int j = 0; j < 9; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));

                // columnName
                r.addColumn(new StringColumn(columnName));
                // timestamp
                r.addColumn(new LongColumn(ts + j));
                // value
                r.addColumn(new LongColumn(j));
                input.add(r);

                row.addAttrColumn(columnName, ColumnValue.fromLong(j), ts + j);
            }

            expect.add(row.toRow());

            for (int j = 9; j < 10; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                r.addColumn(new BytesColumn()); // 空

                // columnName
                r.addColumn(new StringColumn(columnName));
                // timestamp
                r.addColumn(new LongColumn(ts + j));
                // value
                r.addColumn(new LongColumn(j));
                input.add(r);
                rm.add(new RecordAndMessage(r, "The column of record is NULL, primary key name : Mid ."));
            }
        }

        // check
        OTSConf conf = Conf.getConf(
                tableName,
                tableMeta.getPrimaryKeyMap(),
                getColumnMeta(1, ColumnType.INTEGER),
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        test(ots, conf, input, expect, rm, true);
    }

    /**
     * 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。
     * 测试内容：用户配置3列PK，构造10个Cell，其中一个Cell只传入3列PK，但是有一个PK列不能成功的转换为指定的类型，期望该Cell被记录到脏数据回收器中，错误消息符合预期
     *
     * @throws Exception
     */
    @Test
    public void testCase4() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<RecordAndMessage> rm = new ArrayList<RecordAndMessage>();
        // 构造数据
        {
            long ts = System.currentTimeMillis();

            OTSRowBuilder row = OTSRowBuilder.newInstance();
            row.addPrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("Uid_value"));
            row.addPrimaryKeyColumn("Pid", PrimaryKeyValue.fromLong(1));
            row.addPrimaryKeyColumn("Mid", PrimaryKeyValue.fromBinary("Mid_value".getBytes()));

            String columnName = getColumnName(0);
            for (int j = 0; j < 9; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));

                // columnName
                r.addColumn(new StringColumn(columnName));
                // timestamp
                r.addColumn(new LongColumn(ts + j));
                // value
                r.addColumn(new LongColumn(j));
                input.add(r);

                row.addAttrColumn(columnName, ColumnValue.fromLong(j), ts + j);
            }

            expect.add(row.toRow());

            for (int j = 9; j < 10; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                r.addColumn(new LongColumn(1));// 转换失败

                // columnName
                r.addColumn(new StringColumn(columnName));
                // timestamp
                r.addColumn(new LongColumn(ts + j));
                // value
                r.addColumn(new LongColumn(j));
                input.add(r);
                rm.add(new RecordAndMessage(r, "Column coversion error, src type : LONG, src value: 1, expect type: BINARY ."));
            }
        }

        // check
        OTSConf conf = Conf.getConf(
                tableName,
                tableMeta.getPrimaryKeyMap(),
                getColumnMeta(1, ColumnType.INTEGER),
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        test(ots, conf, input, expect, rm, true);
    }

    /**
     * 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。
     * 测试内容：构造10个Cell，其中一个Cell的columnName为空，期望该Cell被记录到脏数据回收器中，错误消息符合预期
     *
     * @throws Exception
     */
    @Test
    public void testCase5() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<RecordAndMessage> rm = new ArrayList<RecordAndMessage>();
        // 构造数据
        {
            long ts = System.currentTimeMillis();

            OTSRowBuilder row = OTSRowBuilder.newInstance();
            row.addPrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("Uid_value"));
            row.addPrimaryKeyColumn("Pid", PrimaryKeyValue.fromLong(1));
            row.addPrimaryKeyColumn("Mid", PrimaryKeyValue.fromBinary("Mid_value".getBytes()));

            String columnName = getColumnName(0);
            for (int j = 0; j < 9; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));

                // columnName
                r.addColumn(new StringColumn(columnName));
                // timestamp
                r.addColumn(new LongColumn(ts + j));
                // value
                r.addColumn(new LongColumn(j));
                input.add(r);

                row.addAttrColumn(columnName, ColumnValue.fromLong(j), ts + j);
            }

            expect.add(row.toRow());

            for (int j = 9; j < 10; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));

                // columnName
                r.addColumn(new StringColumn());
                // timestamp
                r.addColumn(new LongColumn(ts + j));
                // value
                r.addColumn(new LongColumn(j));
                input.add(r);
                rm.add(new RecordAndMessage(r, "The name of column should not be null or empty."));
            }
        }

        // check
        OTSConf conf = Conf.getConf(
                tableName,
                tableMeta.getPrimaryKeyMap(),
                getColumnMeta(1, ColumnType.INTEGER),
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        test(ots, conf, input, expect, rm, true);
    }

    /**
     * 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。
     * 测试内容：构造10个Cell，其中一个Cell的timestamp为空，期望该Cell被记录到脏数据回收器中，错误消息符合预期
     *
     * @throws Exception
     */
    @Test
    public void testCase6() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<RecordAndMessage> rm = new ArrayList<RecordAndMessage>();
        // 构造数据
        {
            long ts = System.currentTimeMillis();

            OTSRowBuilder row = OTSRowBuilder.newInstance();
            row.addPrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("Uid_value"));
            row.addPrimaryKeyColumn("Pid", PrimaryKeyValue.fromLong(1));
            row.addPrimaryKeyColumn("Mid", PrimaryKeyValue.fromBinary("Mid_value".getBytes()));

            String columnName = getColumnName(0);
            for (int j = 0; j < 9; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));

                // columnName
                r.addColumn(new StringColumn(columnName));
                // timestamp
                r.addColumn(new LongColumn(ts + j));
                // value
                r.addColumn(new LongColumn(j));
                input.add(r);

                row.addAttrColumn(columnName, ColumnValue.fromLong(j), ts + j);
            }

            expect.add(row.toRow());

            for (int j = 9; j < 10; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));

                // columnName
                r.addColumn(new StringColumn(columnName));
                // timestamp
                r.addColumn(new LongColumn());
                // value
                r.addColumn(new LongColumn(j));
                input.add(r);
                rm.add(new RecordAndMessage(r, "The input timestamp can not be empty in the multiVersion mode."));
            }
        }

        // check
        OTSConf conf = Conf.getConf(
                tableName,
                tableMeta.getPrimaryKeyMap(),
                getColumnMeta(1, ColumnType.INTEGER),
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        test(ots, conf, input, expect, rm, true);
    }

    /**
     * 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。
     * 测试内容：构造10个Cell，其中一个Cell的timestamp为空，期望该Cell被记录到脏数据回收器中，错误消息符合预期
     *
     * @throws Exception
     */
    @Test
    public void testCase6Conversion() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<RecordAndMessage> rm = new ArrayList<RecordAndMessage>();
        // 构造数据
        {
            long ts = System.currentTimeMillis();

            OTSRowBuilder row = OTSRowBuilder.newInstance();
            row.addPrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("Uid_value"));
            row.addPrimaryKeyColumn("Pid", PrimaryKeyValue.fromLong(1));
            row.addPrimaryKeyColumn("Mid", PrimaryKeyValue.fromBinary("Mid_value".getBytes()));

            String columnName = getColumnName(0);
            for (int j = 0; j < 9; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));

                // columnName
                r.addColumn(new StringColumn(columnName));
                // timestamp
                r.addColumn(new LongColumn(ts + j));
                // value
                r.addColumn(new LongColumn(j));
                input.add(r);

                row.addAttrColumn(columnName, ColumnValue.fromLong(j), ts + j);
            }

            expect.add(row.toRow());

            for (int j = 9; j < 10; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));

                // columnName
                r.addColumn(new StringColumn(columnName));
                // timestamp
                r.addColumn(new StringColumn("hello"));
                // value
                r.addColumn(new LongColumn(j));
                input.add(r);
                rm.add(new RecordAndMessage(r, "Code:[Common-01], Describe:[同步数据出现业务脏数据情况，数据类型转换错误 .] - String[\"hello\"]不能转为Long ."));
            }
        }

        // check
        OTSConf conf = Conf.getConf(
                tableName,
                tableMeta.getPrimaryKeyMap(),
                getColumnMeta(1, ColumnType.INTEGER),
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        test(ots, conf, input, expect, rm, true);
    }

    /**
     * (修改测试行为，value为null，解析为删除)
     * 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。
     * 测试内容：构造10个Cell，其中一个Cell的value为空，ts是第一个cell的ts，期望该Cell被删除
     *
     * @throws Exception
     */
    @Test
    public void testCase7() throws Exception {
        List<Record> input = new ArrayList<Record>();
        List<Row> expect = new ArrayList<Row>();
        List<RecordAndMessage> rm = new ArrayList<RecordAndMessage>();
        // 构造数据
        {
            long ts = System.currentTimeMillis();

            OTSRowBuilder row = OTSRowBuilder.newInstance();
            row.addPrimaryKeyColumn("Uid", PrimaryKeyValue.fromString("Uid_value"));
            row.addPrimaryKeyColumn("Pid", PrimaryKeyValue.fromLong(1));
            row.addPrimaryKeyColumn("Mid", PrimaryKeyValue.fromBinary("Mid_value".getBytes()));

            String columnName = getColumnName(0);
            for (int j = 0; j < 9; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));

                // columnName
                r.addColumn(new StringColumn(columnName));
                // timestamp
                r.addColumn(new LongColumn(ts + j));
                // value
                r.addColumn(new LongColumn(j));
                input.add(r);

                if (j != 0) {
                    row.addAttrColumn(columnName, ColumnValue.fromLong(j), ts + j);
                }
            }

            expect.add(row.toRow());

            // 删除指定的cell
            for (int j = 0; j < 1; j++) {
                Record r = new DefaultRecord();
                // pk
                r.addColumn(new StringColumn("Uid_value"));
                r.addColumn(new LongColumn(1));
                r.addColumn(new BytesColumn("Mid_value".getBytes()));

                // columnName
                r.addColumn(new StringColumn(columnName));
                // timestamp
                r.addColumn(new LongColumn(ts + j));
                // value
                r.addColumn(new LongColumn());
                input.add(r);
                //rm.add(new RecordAndMessage(r, "The input value can not be empty in the multiVersion mode."));
            }
        }

        // check
        OTSConf conf = Conf.getConf(
                tableName,
                tableMeta.getPrimaryKeyMap(),
                getColumnMeta(1, ColumnType.INTEGER),
                OTSOpType.UPDATE_ROW,
                OTSMode.MULTI_VERSION);
        conf.getRestrictConf().setRowCellCountLimitation(1);
        test(ots, conf, input, expect, rm, true);
    }
}
