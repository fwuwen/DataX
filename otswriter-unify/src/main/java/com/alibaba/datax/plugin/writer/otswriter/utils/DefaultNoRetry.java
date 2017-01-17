package com.alibaba.datax.plugin.writer.otswriter.utils;

import com.alicloud.openservices.tablestore.model.RetryStrategy;


public class DefaultNoRetry implements RetryStrategy {

    @Override
    public RetryStrategy clone() {
        return this;
    }

    @Override
    public int getRetries() {
        return 0;
    }

    @Override
    public long nextPause(String s, Exception e) {
        return 0;
    }
}