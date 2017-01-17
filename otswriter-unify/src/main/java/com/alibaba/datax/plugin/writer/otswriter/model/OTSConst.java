package com.alibaba.datax.plugin.writer.otswriter.model;

public class OTSConst {
    // Reader support type
    public final static String TYPE_STRING  = "STRING";
    public final static String TYPE_INTEGER = "INT";
    public final static String TYPE_DOUBLE  = "DOUBLE";
    public final static String TYPE_BOOLEAN = "BOOL";
    public final static String TYPE_BINARY  = "BINARY";
    
    // Column
    public final static String NAME = "name";
    public final static String SRC_NAME = "srcName";
    public final static String TYPE = "type";
    
    public final static String OTS_CONF = "OTS_CONF";
    
    public final static String OTS_MODE_NORMAL = "normal";
    public final static String OTS_MODE_MULTI_VERSION = "multiVersion";
    
    public final static String OTS_OP_TYPE_PUT = "PutRow";
    public final static String OTS_OP_TYPE_UPDATE = "UpdateRow";
    
    // options
    public final static String RETRY = "maxRetryTime";
    public final static String SLEEP_IN_MILLISECOND = "retrySleepInMillisecond";
    public final static String BATCH_WRITE_COUNT = "batchWriteCount";
    public final static String CONCURRENCY_WRITE = "concurrencyWrite";
    public final static String IO_THREAD_COUNT = "ioThreadCount";
    public final static String MAX_CONNECT_COUNT = "maxConnectCount";
    public final static String SOCKET_TIMEOUTIN_MILLISECOND = "socketTimeoutInMillisecond";
    public final static String CONNECT_TIMEOUT_IN_MILLISECOND = "connectTimeoutInMillisecond";
    
    // 限制项
    public final static String REQUEST_TOTAL_SIZE_LIMITATION = "requestTotalSizeLimitation";
    public final static String ROW_CELL_COUNT_LIMITATION = "rowCellCountLimitation";
}
