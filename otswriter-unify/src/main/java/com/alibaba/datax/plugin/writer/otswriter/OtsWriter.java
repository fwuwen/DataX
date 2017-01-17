package com.alibaba.datax.plugin.writer.otswriter;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSMode;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.alicloud.openservices.tablestore.*;

public class OtsWriter {
    public static class Job extends Writer.Job  {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private OtsWriterMasterProxy proxy = new OtsWriterMasterProxy();
        
        @Override
        public void init() {
            LOG.info("init() begin ...");
            try {
                this.proxy.init(getPluginJobConf());
            } catch (TableStoreException e) {
                LOG.error("TableStoreException: {}",  e.toString(), e);
                throw DataXException.asDataXException(new OtsWriterError(e.getErrorCode(), "OTS Client Error"), e.toString(), e);
            } catch (ClientException e) {
                LOG.error("ClientException: {}",  e.toString(), e);
                throw DataXException.asDataXException(OtsWriterError.ERROR, e.toString(), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.toString(), e);
                throw DataXException.asDataXException(OtsWriterError.ERROR, e.toString(), e);
            }
            LOG.info("init() end ...");
        }

        @Override
        public void destroy() {
            this.proxy.close();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            try {
                return this.proxy.split(mandatoryNumber);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsWriterError.ERROR, e.toString(), e);
            }
        }
    }
    
    public static class Task extends Writer.Task  {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private OtsWriterSlaveProxy proxy = null;
        private SyncClientInterface ots = null;
        
        /**
         * 基于配置，构建对应的worker代理
         */
        @Override
        public void init() {
            
            // get conf
            OTSConf conf = GsonParser.jsonToConf(this.getPluginJobConf().getString(OTSConst.OTS_CONF));
            ots = Common.getOTSInstance(conf);
            
            if (conf.getMode() == OTSMode.MULTI_VERSION) {
                LOG.info("init OtsWriterSlaveProxyMultiversion");
                proxy = new OtsWriterSlaveProxyMultiversion(ots, conf);
            } else {
                LOG.info("init OtsWriterSlaveProxyNormal");
                proxy = new OtsWriterSlaveProxyNormal(ots, conf);
            }
            
            proxy.init(this.getTaskPluginCollector());
        }

        @Override
        public void destroy() {
            try {
                this.proxy.close();
                ots.shutdown();
            } catch (OTSCriticalException e) {
                LOG.error("OTSCriticalException. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsWriterError.ERROR, e.toString(), e);
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.info("startWrite() begin ...");
            try {
                this.proxy.write(lineReceiver);
            } catch (TableStoreException e) {
                LOG.error("TableStoreException: {}",  e.toString(), e);
                throw DataXException.asDataXException(new OtsWriterError(e.getErrorCode(), "OTS Client Error"), e.toString(), e);
            } catch (ClientException e) {
                LOG.error("ClientException: {}",  e.toString(), e);
                throw DataXException.asDataXException(OtsWriterError.ERROR, e.toString(), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.toString(), e);
                throw DataXException.asDataXException(OtsWriterError.ERROR, e.toString(), e);
            }
            LOG.info("startWrite() end ...");
        }
    }
}
