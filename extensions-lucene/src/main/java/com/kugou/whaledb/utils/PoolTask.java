package com.kugou.whaledb.utils;

import org.apache.log4j.Logger;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


public abstract class PoolTask extends java.util.TimerTask {
    private ThreadPoolExecutor EXECUTE;

    public abstract void execute();

    private AtomicBoolean isRunning = new AtomicBoolean(false);
    private AtomicLong runStartTs = new AtomicLong(System.currentTimeMillis());
    private static Logger LOG = Logger.getLogger(PoolTask.class);

    private static long TIMEOUT = 700000L;


    public void setEXECUTE(ThreadPoolExecutor paramThreadPoolExecutor) {
        this.EXECUTE = paramThreadPoolExecutor;
    }


    public String logMsg() {
        return "";
    }

    public void run() {
        this.EXECUTE.submit(new NamedThreadPool.RunWithName() {
            @Override
            public String debugmsg() {
                return getClass().getName() + ",msg:" + logMsg();
            }

            @Override
            public void run() {
                long l = System.currentTimeMillis();
                if (isRunning.get() && (l - runStartTs.get() < TIMEOUT)) {
                    LOG.info("PoolTaskBusy:" + getClass().getName() + ",msg:" + logMsg());
                    return;
                }
                if (isRunning.get()) {
                    LOG.info("PoolTaskTimeout:" + getClass().getName() + ",msg:" + logMsg());
                }
                runStartTs.set(l);
                isRunning.set(true);
                try {
                    execute();
                } finally {
                    isRunning.set(false);
                }
            }
        });
    }
}