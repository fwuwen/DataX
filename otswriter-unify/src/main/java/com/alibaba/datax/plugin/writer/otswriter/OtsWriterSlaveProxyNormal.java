package com.alibaba.datax.plugin.writer.otswriter;

import java.util.Map;

import com.alibaba.datax.plugin.writer.otswriter.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.utils.CollectorUtil;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.alibaba.datax.plugin.writer.otswriter.utils.ParseRecord;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;

public class OtsWriterSlaveProxyNormal implements OtsWriterSlaveProxy {
    
    private OTSConf conf = null;
    private SyncClientInterface ots = null;
    private OTSSendBuffer buffer = null;
    private Map<TablePrimaryKeySchema, Integer> pkColumnMapping = null;
    private static final Logger LOG = LoggerFactory.getLogger(OtsWriterSlaveProxyNormal.class);
    
    public OtsWriterSlaveProxyNormal(SyncClientInterface ots, OTSConf conf) {
        this.ots = ots;
        this.conf = conf;
        this.pkColumnMapping = Common.getPkColumnMapping(conf.getEncodePkColumnMapping());
    } 
    
    @Override
    public void init(TaskPluginCollector taskPluginCollector) {
        LOG.info("init begin");
        // 初始化全局垃圾回收器
        CollectorUtil.init(taskPluginCollector);
        buffer = new OTSSendBuffer(ots, conf);
        LOG.info("init end");
    }

    @Override
    public void close() throws OTSCriticalException {
        LOG.info("close begin");
        buffer.close();
        LOG.info("close end");
    }
    
    @Override
    public void write(RecordReceiver recordReceiver) throws OTSCriticalException {
        LOG.info("write begin");
        int expectColumnCount = conf.getPrimaryKeyColumn().size() + conf.getAttributeColumn().size();
        Record record = null;
        
        while ((record = recordReceiver.getFromReader()) != null) {
            
            LOG.debug("Record Raw: {}", record.toString());
            
            int columnCount = record.getColumnNumber();
            if (columnCount != expectColumnCount) {
                // 如果Column的个数和预期的个数不一致时，认为是系统故障或者用户配置Column错误，异常退出
                throw new OTSCriticalException(String.format(
                        OTSErrorMessage.RECORD_AND_COLUMN_SIZE_ERROR, 
                        columnCount, 
                        expectColumnCount,
                        record.toString()
                        ));
            }
            
            OTSLine line = ParseRecord.parseNormalRecordToOTSLine(
                    conf.getTableName(), 
                    conf.getOperation(), 
                    pkColumnMapping,
                    conf.getAttributeColumn(), 
                    record,
                    conf.getTimestamp());
            if (line != null) {
                buffer.write(line);
            }
        }
        LOG.info("write end");
    }
}
