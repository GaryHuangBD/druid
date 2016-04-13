package com.kugou.whaledb.utils;

import java.util.Timer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class TimerPools {
    private static ThreadPoolExecutor EXECUTE = null;
    private static Object EXELOCK = new Object();
    private Timer timer;

    public TimerPools(String paramString) {
        this.timer = new Timer(paramString);
        synchronized (EXELOCK) {
            if (EXECUTE == null) {
                EXECUTE = NamedThreadPool.createHigh(3, Integer.MAX_VALUE, 600L, java.util.concurrent.TimeUnit.SECONDS, new LinkedBlockingQueue(), paramString + "_tpool");
            }
        }
    }

    public void schedule(PoolTask paramPoolTask, long paramLong1, long paramLong2) {
        synchronized (EXELOCK) {
            paramPoolTask.setEXECUTE(EXECUTE);
        }
        timer.schedule(paramPoolTask, paramLong1 + (long)(Math.random() * paramLong1 / 10.0D), paramLong2 + (long)(Math.random() * paramLong2 / 10.0D));
    }

    public void schedule(PoolTask paramPoolTask, long paramLong) {
        synchronized (EXELOCK) {
            paramPoolTask.setEXECUTE(EXECUTE);
        }
        timer.schedule(paramPoolTask, paramLong + (long)(Math.random() * paramLong / 10.0D));
    }

    public int purge() {
        return timer.purge();
    }
}
