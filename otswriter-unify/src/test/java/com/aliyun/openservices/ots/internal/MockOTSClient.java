package com.aliyun.openservices.ots.internal;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.model.Error;
import com.aliyun.openservices.ots.internal.model.MockBatchWriteRowResult;
import com.aliyun.openservices.ots.internal.model.MockGetRangeResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alicloud.openservices.tablestore.model.*;

/**
 * Mock OTS Client
 *
 * @author redchen
 */
public class MockOTSClient implements SyncClientInterface {

    private AtomicInteger invokeTimes = new AtomicInteger(0); // 调用次数
    private AtomicInteger conInvokeTimes = new AtomicInteger(0); // 并发调用次数
    private int conMaxInvokeTimes = 0; // 最大的并发调用次数
    private Exception exception = null; // 异常设置

    private Lock lock = new ReentrantLock();

    private long writeCU = 0; // 写CU
    private long remaingCU = 0; // 剩余写CU
    private long lastTime = 0; // 上次操作的时间
    private long elapsedTime = 0; // 操作执行的时间

    private List<Integer> rows = new ArrayList<Integer>(); // 记录每次操作的函数

    private Map<PrimaryKey, Row> lines = new HashMap<PrimaryKey, Row>(); //

    private static final Logger LOG = LoggerFactory.getLogger(MockOTSClient.class);


    public MockOTSClient() {
        this(5000, null, null);
    }

    public MockOTSClient(
            int writeCU,
            Exception exception,
            Map<PrimaryKey, Row> prepare
    ) {
        this(writeCU, exception, prepare, 10);
    }

    public MockOTSClient(
            int writeCU,
            Exception exception,
            Map<PrimaryKey, Row> prepare,
            long elapsedTime
    ) {
        this.writeCU = writeCU;
        this.remaingCU = writeCU;
        this.lastTime = (new Date()).getTime();
        this.exception = exception;
        this.elapsedTime = elapsedTime;
        if (prepare != null) {
            lines.putAll(prepare);
        }
    }

    public int getInvokeTimes() {
        return invokeTimes.intValue();
    }

    public int getMaxConcurrenyInvokeTimes() {
        return conMaxInvokeTimes;
    }

    public Map<PrimaryKey, Row> getData() {
        return lines;
    }

    /**
     * 每次操作的行数
     *
     * @return
     */
    public List<Integer> getRowsCountPerRequest() {
        return rows;
    }

    private void add(OTSOpType type, PrimaryKey pk, List<Pair<Column, RowUpdateChange.Type>> attr, long ts) {
        if (type == OTSOpType.PUT_ROW) {
            lines.put(pk, new Row(pk, toColumns(attr, ts)));
        } else {
            Row old = lines.get(pk);
            if (old == null) {
                List<Column> columns = new ArrayList<Column>();

                for (Pair<Column, RowUpdateChange.Type> p : attr) {
                    if (p.getSecond() == RowUpdateChange.Type.PUT) {
                        Column c = p.getFirst();
                        if (!c.hasSetTimestamp()) {
                            columns.add(new Column(c.getName(), c.getValue(), ts));
                        } else {
                            columns.add(p.getFirst());
                        }
                    }
                }
                lines.put(pk, new Row(pk, columns));
            } else {
                // merge
                NavigableMap<String, NavigableMap<Long, ColumnValue>> mapping = old.getColumnsMap();
                for (Pair<Column, RowUpdateChange.Type> p : attr) {
                    if (p.getSecond() == RowUpdateChange.Type.PUT) {
                        NavigableMap<Long, ColumnValue> cells = mapping.get(p.getFirst().getName());
                        if (cells != null) {
                            if (!p.getFirst().hasSetTimestamp()) {
                                cells.put(ts, p.getFirst().getValue());
                            } else {
                                cells.put(p.getFirst().getTimestamp(), p.getFirst().getValue());
                            }
                        } else {
                            cells = new TreeMap<Long, ColumnValue>(new Comparator<Long>() {
                                public int compare(Long l1, Long l2) {
                                    return l2.compareTo(l1);
                                }
                            });
                            cells.put(p.getFirst().getTimestamp(), p.getFirst().getValue());
                            mapping.put(p.getFirst().getName(), cells);
                        }

                    } else if (p.getSecond() == RowUpdateChange.Type.DELETE_ALL) {
                        mapping.remove(p.getFirst().getName());
                    } else {
                        mapping.get(p.getFirst().getName()).remove(p.getFirst().getTimestamp());
                    }
                }
                List<Column> columns = new ArrayList<Column>();
                for (Entry<String, NavigableMap<Long, ColumnValue>> en : mapping.entrySet()) {
                    for (Entry<Long, ColumnValue> ennn : en.getValue().entrySet()) {
                        columns.add(new Column(en.getKey(), ennn.getValue(), ennn.getKey()));
                    }
                }
                lines.put(pk, new Row(pk, columns));
            }
        }
    }

    private List<Column> toColumns(List<Pair<Column, RowUpdateChange.Type>> attr, long ts) {
        List<Column> r = new ArrayList<Column>();
        for (Pair<Column, RowUpdateChange.Type> p : attr) {
            Column c = p.getFirst();
            if (!c.hasSetTimestamp()) {
                r.add(new Column(c.getName(), c.getValue(), ts));
            } else {
                r.add(p.getFirst());
            }
        }
        return r;
    }

    private List<Pair<Column, RowUpdateChange.Type>> toPairColumns(List<Column> attr) {
        List<Pair<Column, RowUpdateChange.Type>> r = new ArrayList<Pair<Column, RowUpdateChange.Type>>();
        for (Column p : attr) {
            r.add(new Pair<Column, RowUpdateChange.Type>(p, RowUpdateChange.Type.PUT));
        }
        return r;
    }

    private void send(OTSOpType type, String tableName, PrimaryKey pk, List<Pair<Column, RowUpdateChange.Type>> attr, long ts) throws TableStoreException {
        long expectCU = 10;

        try {
            lock.lock();
            long curTime = (new Date()).getTime();
            long rangeTime = curTime - lastTime;//上次操作的间隔

            long tmpCU = writeCU * rangeTime / 1000;
            long tmpRemaingCU = (remaingCU + tmpCU) < writeCU ? (remaingCU + tmpCU) : writeCU; // 计算CU
            if ((tmpRemaingCU - 1) >= 0) { // 预扣CU
                lastTime = curTime;
                remaingCU = remaingCU - expectCU + tmpCU; // 补扣CU
                // add data
                add(type, pk, attr, ts);
                try {
                    Thread.sleep(elapsedTime);
                } catch (InterruptedException e) {
                }
            } else {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                }
                throw new RuntimeException(OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT);
            }
        } finally {
            lock.unlock();
        }
    }

    private void handleRowChange(Map<String, List<RowChange>> input, MockBatchWriteRowResult result) throws InterruptedException, TableStoreException {
        // mock ots
        int totalRow = 0;
        int totalSize = 0;
        for (Entry<String, List<RowChange>> en : input.entrySet()) {
            // Row count的检查
            totalRow += en.getValue().size();
            if (totalRow > 100) {
                throw new TableStoreException(
                        "Total Row count > 100",
                        null,
                        OTSErrorCode.INVALID_PARAMETER,
                        "RequestId",
                        400);
            }

            for (RowChange rc : en.getValue()) {
                totalSize += Helper.getPKSize(rc.getPrimaryKey());

                if (rc instanceof RowPutChange) {
                    RowPutChange change = (RowPutChange) rc;
                    // column number的检查
                    if (change.getColumnsToPut().size() > 128) {
                        throw new TableStoreException(
                                "Attribute column > 128",
                                null,
                                OTSErrorCode.INVALID_PARAMETER,
                                "RequestId",
                                400);
                    }

                    // column name 合法性检查
                    totalSize += Helper.getAttrSize(change.getColumnsToPut());

                    // Total Size的检查
                    if (totalSize > (1024 * 1024)) {
                        throw new TableStoreException(
                                "Total Size > 1MB",
                                null,
                                OTSErrorCode.INVALID_PARAMETER,
                                "RequestId",
                                400);
                    }
                } else if (rc instanceof RowUpdateChange) {
                    RowUpdateChange change = (RowUpdateChange) rc;
                    totalSize += Helper.getPKSize(change.getPrimaryKey());

                    // column number的检查
                    if (change.getColumnsToUpdate().size() > 128) {
                        throw new TableStoreException(
                                "Attribute column > 128",
                                null,
                                OTSErrorCode.INVALID_PARAMETER,
                                "RequestId",
                                400);
                    }


                    // column name 合法性检查
                    totalSize += Helper.getAttrSize(toColumns(change.getColumnsToUpdate(), 0L));

                    // Total Size的检查
                    if (totalSize > (1024 * 1024)) {
                        throw new TableStoreException(
                                "Total Size > 1MB",
                                null,
                                OTSErrorCode.INVALID_PARAMETER,
                                "RequestId",
                                400);
                    }
                }
            }

        }

        // mock sql clinet
        // STRING PK, STRING ATTR, BINARY ATTR的检查
        long ts = System.currentTimeMillis();
        int index = 0;
        for (Entry<String, List<RowChange>> en : input.entrySet()) {
            for (RowChange rc : en.getValue()) {// row
                if (rc instanceof RowPutChange) {
                    RowPutChange change = (RowPutChange) rc;
                    boolean flag = true;
                    for (PrimaryKeyColumn pk : change.getPrimaryKey().getPrimaryKeyColumns()) {
                        switch (pk.getValue().getType()) {
                            case STRING:
                                if (pk.getValue().asString().length() > 1024) {
                                    result.addRowResult(
                                            new BatchWriteRowResponse.RowResult(
                                                    en.getKey(),
                                                    null,
                                                    new com.alicloud.openservices.tablestore.model.Error(OTSErrorCode.INVALID_PARAMETER, "STRING PK SIZE > 1KB"),
                                                    index
                                            )
                                    );
                                    flag = false;
                                }
                                break;
                            default:
                                break;
                        }
                    }

                    for (Column attr : change.getColumnsToPut()) {
                        switch (attr.getValue().getType()) {
                            case BINARY:
                                if (attr.getValue().asBinary().length > 64 * 1024) {
                                    result.addRowResult(
                                            new BatchWriteRowResponse.RowResult(
                                                    en.getKey(),
                                                    null,
                                                    new Error(OTSErrorCode.INVALID_PARAMETER, "BINARY ATTR SIZE > 64KB"),
                                                    index
                                            ));
                                    flag = false;
                                }
                                break;
                            case STRING:
                                if (attr.getValue().asString().length() > 64 * 1024) {
                                    result.addRowResult(
                                            new BatchWriteRowResponse.RowResult(
                                                    en.getKey(),
                                                    null,
                                                    new Error(OTSErrorCode.INVALID_PARAMETER, "STRING ATTR SIZE > 64KB"),
                                                    index
                                            ));
                                    flag = false;
                                }
                                break;
                            default:
                                break;
                        }

                    }
                    if (flag) {
                        // send to worker
                        // mock sql worker
                        try {
                            send(OTSOpType.PUT_ROW, change.getTableName(), change.getPrimaryKey(), toPairColumns(change.getColumnsToPut()), ts);
                            result.addRowResult(
                                    new BatchWriteRowResponse.RowResult(
                                            change.getTableName(),
                                            null,
                                            new ConsumedCapacity(new CapacityUnit(0, 1)),
                                            index
                                    )
                            );
                        } catch (RuntimeException e) {
                            LOG.warn("RuntimeException:{}", e.getMessage(), e);
                            result.addRowResult(
                                    new BatchWriteRowResponse.RowResult(
                                            change.getTableName(),
                                            null,
                                            new Error(OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT, "CU NOT ENOUGH"),
                                            index));
                        }
                    }
                    index++;
                } else if (rc instanceof RowUpdateChange) {
                    RowUpdateChange change = (RowUpdateChange) rc;
                    boolean flag = true;
                    for (PrimaryKeyColumn pk : change.getPrimaryKey().getPrimaryKeyColumns()) {
                        switch (pk.getValue().getType()) {
                            case STRING:
                                if (pk.getValue().asString().length() > 1024) {
                                    result.addRowResult(
                                            new BatchWriteRowResponse.RowResult(
                                                    en.getKey(),
                                                    null,
                                                    new Error(OTSErrorCode.INVALID_PARAMETER, "STRING PK SIZE > 1KB"),
                                                    index
                                            ));
                                    flag = false;
                                }
                                break;
                            default:
                                break;
                        }
                    }
                    for (Pair<Column, RowUpdateChange.Type> attr : change.getColumnsToUpdate()) {
                        switch (attr.getFirst().getValue().getType()) {
                            case BINARY:
                                if (attr.getFirst().getValue().asBinary().length > 64 * 1024) {
                                    result.addRowResult(
                                            new BatchWriteRowResponse.RowResult(
                                                    en.getKey(),
                                                    null,
                                                    new Error(OTSErrorCode.INVALID_PARAMETER, "BINARY ATTR SIZE > 64KB"),
                                                    index
                                            ));
                                    flag = false;
                                }

                                break;
                            case STRING:
                                if (attr.getFirst().getValue().asString() != null && attr.getFirst().getValue().asString().length() > 64 * 1024) {
                                    result.addRowResult(
                                            new BatchWriteRowResponse.RowResult(
                                                    en.getKey(),
                                                    null,
                                                    new Error(OTSErrorCode.INVALID_PARAMETER, "STRING ATTR SIZE > 64KB"),
                                                    index
                                            ));
                                    flag = false;
                                }
                                break;
                            default:
                                break;
                        }
                    }
                    if (flag) {
                        // send to worker
                        // mock worker
                        try {
                            send(OTSOpType.UPDATE_ROW, change.getTableName(), change.getPrimaryKey(), change.getColumnsToUpdate(), ts);
                            result.addRowResult(
                                    new BatchWriteRowResponse.RowResult(
                                            change.getTableName(),
                                            null,
                                            new ConsumedCapacity(new CapacityUnit(0, 1)),
                                            index));
                        } catch (RuntimeException e) {
                            LOG.warn("RuntimeException:{}", e.getMessage(), e);
                            result.addRowResult(
                                    new BatchWriteRowResponse.RowResult(
                                            change.getTableName(),
                                            null,
                                            new Error(OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT, "CU NOT ENOUGH"),
                                            index
                                    ));
                        }
                    }
                }
            }
        }
    }

    /**
     * PutRow 检查流程
     * OTS
     * 1.Column的个数检查
     * 2.ColumnName的合法性检查
     * <p/>
     * SQL CLIENT
     * 1.Cell Value长度的检查
     * <p/>
     * SQL WORKER
     * 1.CU计算
     * 2.写入数据/抛异常
     */
    @Override
    public PutRowResponse putRow(PutRowRequest putRowRequest)
            throws TableStoreException, ClientException {
        try {
            invokeTimes.incrementAndGet();
            conInvokeTimes.incrementAndGet();
            rows.add(1);

            long ts = System.currentTimeMillis();

            if (conInvokeTimes.intValue() > conMaxInvokeTimes) {
                conMaxInvokeTimes = conInvokeTimes.intValue();
            }

            if (exception != null) {
                if (exception instanceof ClientException) {
                    throw (ClientException) exception;
                } else {
                    throw (TableStoreException) exception;
                }
            }

            RowPutChange change = putRowRequest.getRowChange();

            // mock ots
            // column number的检查
            if (change.getColumnsToPut().size() > 128) {
                throw new TableStoreException(
                        "Attribute column > 128",
                        null,
                        OTSErrorCode.INVALID_PARAMETER,
                        "RequestId",
                        400);
            }

            for (Column c : change.getColumnsToPut()) {
                if (!Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*").matcher(c.getName()).matches()) {
                    throw new TableStoreException(
                            "Column name invalid",
                            null,
                            OTSErrorCode.INVALID_PARAMETER,
                            "RequestId",
                            400);
                }
            }

            // mock sql client
            for (PrimaryKeyColumn pkc : change.getPrimaryKey().getPrimaryKeyColumns()) {
                switch (pkc.getValue().getType()) {
                    case STRING:
                        if (pkc.getValue().asString().length() > 1024) {
                            throw new TableStoreException("STRING PK SIZE > 1KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    default:
                        break;
                }
            }
            for (Column attr : change.getColumnsToPut()) {
                switch (attr.getValue().getType()) {
                    case BINARY:
                        if (attr.getValue().asBinary().length > 64 * 1024) {
                            throw new TableStoreException("BINARY ATTR SIZE > 64KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    case STRING:
                        if (attr.getValue().asString().length() > 64 * 1024) {
                            throw new TableStoreException("STRING ATTR SIZE > 64KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    default:
                        break;
                }
            }


            try {
                send(OTSOpType.PUT_ROW, change.getTableName(), change.getPrimaryKey(), toPairColumns(change.getColumnsToPut()), ts);
                Response meta = new Response();
                meta.setRequestId("requsetid put row");
                meta.setTraceId("tracerid");
                return new PutRowResponse(
                        meta,
                        null,
                        new ConsumedCapacity(new CapacityUnit(0, 1))
                );
            } catch (RuntimeException e) {
                throw new TableStoreException("CU NOT ENOUGH", null, OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT, "request_id", 403);
            }
        } finally {
            conInvokeTimes.decrementAndGet();
        }
    }

    @Override
    public UpdateRowResponse updateRow(UpdateRowRequest updateRowRequest)
            throws TableStoreException, ClientException {
        try {
            invokeTimes.incrementAndGet();
            conInvokeTimes.incrementAndGet();
            rows.add(1);

            if (conInvokeTimes.intValue() > conMaxInvokeTimes) {
                conMaxInvokeTimes = conInvokeTimes.intValue();
            }

            if (exception != null) {
                if (exception instanceof ClientException) {
                    throw (ClientException) exception;
                } else {
                    throw (TableStoreException) exception;
                }
            }

            RowUpdateChange change = updateRowRequest.getRowChange();

            // mock ots
            long ts = System.currentTimeMillis();
            // column number的检查
            if (change.getColumnsToUpdate().size() > 128) {
                throw new TableStoreException(
                        "Attribute column > 128",
                        null,
                        OTSErrorCode.INVALID_PARAMETER,
                        "RequestId",
                        400);
            }

            for (Pair<Column, RowUpdateChange.Type> pair : change.getColumnsToUpdate()) {
                if (!Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*").matcher(pair.getFirst().getName()).matches()) {
                    throw new TableStoreException(
                            "Column name invalid",
                            null,
                            OTSErrorCode.INVALID_PARAMETER,
                            "RequestId",
                            400);
                }
            }

            // mock sql client
            for (PrimaryKeyColumn pkc : change.getPrimaryKey().getPrimaryKeyColumns()) {
                switch (pkc.getValue().getType()) {
                    case STRING:
                        if (pkc.getValue().asString().length() > 1024) {
                            throw new TableStoreException("STRING PK SIZE > 1KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    default:
                        break;
                }
            }

            for (Pair<Column, RowUpdateChange.Type> pair : change.getColumnsToUpdate()) {
                switch (pair.getFirst().getValue().getType()) {
                    case BINARY:
                        if (pair.getFirst().getValue().asBinary().length > 64 * 1024) {
                            throw new TableStoreException("BINARY ATTR SIZE > 64KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    case STRING:
                        if (pair.getFirst().getValue().asString().length() > 64 * 1024) {
                            throw new TableStoreException("STRING ATTR SIZE > 64KB", null, OTSErrorCode.INVALID_PARAMETER, "request_id", 400);
                        }
                        break;
                    default:
                        break;
                }
            }

            try {
                send(OTSOpType.UPDATE_ROW, change.getTableName(), change.getPrimaryKey(), change.getColumnsToUpdate(), ts);
                Response meta = new Response();
                meta.setRequestId("requsetid update row");
                meta.setTraceId("tracerid");
                return new UpdateRowResponse(meta, null, new ConsumedCapacity(new CapacityUnit(0, 1)));
            } catch (RuntimeException e) {
                throw new TableStoreException("CU NOT ENOUGH", null, OTSErrorCode.NOT_ENOUGH_CAPACITY_UNIT, "request_id", 403);
            }
        } finally {
            conInvokeTimes.decrementAndGet();
        }
    }

    @Override
    public BatchWriteRowResponse batchWriteRow(
            BatchWriteRowRequest batchWriteRowRequest) throws TableStoreException,
            ClientException {
        try {
            invokeTimes.incrementAndGet();
            conInvokeTimes.incrementAndGet();

            int rowsCount = 0;
            for (Entry<String, List<RowChange>> en : batchWriteRowRequest.getRowChange().entrySet()) {
                rowsCount += en.getValue().size();
            }

            rows.add(rowsCount);

            if (conInvokeTimes.intValue() > conMaxInvokeTimes) {
                conMaxInvokeTimes = conInvokeTimes.intValue();
            }

            if (exception != null) {
                if (exception instanceof ClientException) {
                    throw (ClientException) exception;
                } else {
                    throw (TableStoreException) exception;
                }
            }

            Response meta = new Response();
            meta.setRequestId("requsetid batch write row");
            meta.setTraceId("tracerid");
            MockBatchWriteRowResult result = new MockBatchWriteRowResult(meta);

            try {
                handleRowChange(batchWriteRowRequest.getRowChange(), result);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage());
            }
            return result;
        } finally {
            conInvokeTimes.decrementAndGet();
        }
    }

    @Override
    public GetRangeResponse getRange(GetRangeRequest getRangeRequest)
            throws TableStoreException, ClientException {

        List<Row> rows = new ArrayList<Row>();
        for (Row r : lines.values()) {
            if (r.getColumns().length != 0) {
                rows.add(r);
            }
        }

        MockGetRangeResult result = new MockGetRangeResult(rows);
        return result.toGetRangeResult();
    }

    // ########################################################################

    @Override
    public CreateTableResponse createTable(CreateTableRequest createTableRequest)
            throws TableStoreException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public UpdateTableResponse updateTable(UpdateTableRequest updateTableRequest)
            throws TableStoreException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public DescribeTableResponse describeTable(
            DescribeTableRequest describeTableRequest) throws TableStoreException,
            ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public ListTableResponse listTable() throws TableStoreException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public DeleteTableResponse deleteTable(DeleteTableRequest deleteTableRequest)
            throws TableStoreException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public GetRowResponse getRow(GetRowRequest getRowRequest)
            throws TableStoreException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public DeleteRowResponse deleteRow(DeleteRowRequest deleteRowRequest)
            throws TableStoreException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public BatchGetRowResponse batchGetRow(BatchGetRowRequest batchGetRowRequest)
            throws TableStoreException, ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public Iterator<Row> createRangeIterator(
            RangeIteratorParameter rangeIteratorParameter) throws TableStoreException,
            ClientException {
        throw new RuntimeException("Unimplements");
    }

    @Override
    public WideColumnIterator createWideColumnIterator(GetRowRequest getRowRequest) throws TableStoreException, ClientException {
        return null;
    }

    @Override
    public void shutdown() {
    }

}
