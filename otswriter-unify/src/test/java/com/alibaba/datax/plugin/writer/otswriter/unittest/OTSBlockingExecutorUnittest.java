package com.alibaba.datax.plugin.writer.otswriter.unittest;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSBlockingExecutor;

/**
 * 测试目的：
 * 1.并发资源控制是否准确
 * 2.能否保证所以的Task都能被执行
 * 3.中途关闭OTSBlockingExecutor是否会收到预期的异常
 */
public class OTSBlockingExecutorUnittest {

    private static final Logger LOG = LoggerFactory.getLogger(OTSBlockingExecutor.class);

    private AtomicInteger invokeTimes = new AtomicInteger(0);
    private AtomicInteger concurrencyInvokeTimes = new AtomicInteger(0);
    private int maxConcurrency = 0;
    private OTSBlockingExecutor executor;

    class Worker implements Runnable {

        @Override
        public void run() {
            concurrencyInvokeTimes.incrementAndGet();
            invokeTimes.incrementAndGet();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage());
            }
            if (concurrencyInvokeTimes.intValue() > maxConcurrency) {
                maxConcurrency = concurrencyInvokeTimes.intValue();
            }
            concurrencyInvokeTimes.decrementAndGet();
        }
    }

    class Killer extends Thread {
        @Override
        public void run() {
            try {
                Thread.sleep(3000);
                executor.shutdown();
            } catch (InterruptedException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    // 构造并发N的executor，持续写入压力，测试Task的并发和最终调用次数是否符合期望
    @Test
    public void testMultiConcurrency() throws InterruptedException {

        for (int c = 1; c < 10; c++) {
            maxConcurrency = 0;
            concurrencyInvokeTimes = new AtomicInteger(0);
            invokeTimes = new AtomicInteger(0);
            executor = new OTSBlockingExecutor(c);
            for (int i = 0; i < 50; i++) {
                executor.execute(new Worker());
            }
            executor.shutdown();
            assertEquals(c, maxConcurrency);
            assertEquals(50, invokeTimes.intValue());
        }
    }

    // 构造并发为5的executor，持续的写入压力，在压力写入过程中主动关闭executor，期望调用者能够获得executor关闭的异常消息
    @Test
    public void testForException() throws InterruptedException {
        executor = new OTSBlockingExecutor(5);
        Killer k = new Killer();
        k.start();// sleep 3秒之后关闭executor
        try {
            int i = 0;
            while (i < 10000) {
                executor.execute(new Worker());
            }
            assertTrue(false);
        } catch (RuntimeException e) {
            assertEquals("Can not execute the task, becase the ExecutorService is shutdown.", e.getMessage());
        }
    }
}
