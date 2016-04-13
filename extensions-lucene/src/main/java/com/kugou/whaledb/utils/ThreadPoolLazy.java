package com.kugou.whaledb.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolLazy {
    private ThreadPoolExecutor EXECUTE_THREAD_POOL_FETCH = null;
    private Object EXECUTE_THREAD_POOL_FETCH_LOCK = new Object();
    int corePoolSize;
    int maximumPoolSize;

    public ThreadPoolLazy(int paramInt1, int paramInt2, long paramLong, String paramString) {
        this.corePoolSize = paramInt1;
        this.maximumPoolSize = paramInt2;
        this.keepAliveTime = paramLong;
        this.name = paramString;
    }

    long keepAliveTime;
    String name;

    public ThreadPoolExecutor get() {
        synchronized (this.EXECUTE_THREAD_POOL_FETCH_LOCK) {
            if (this.EXECUTE_THREAD_POOL_FETCH == null) {
                System.out.println(this.corePoolSize + "," + this.maximumPoolSize + "," + this.keepAliveTime + "," + "ThreadPoolLazy_" + this.name);
                this.EXECUTE_THREAD_POOL_FETCH = NamedThreadPool.createHigh(this.corePoolSize, this.maximumPoolSize, this.keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue(), "ThreadPoolLazy_" + this.name);
            }
            return this.EXECUTE_THREAD_POOL_FETCH;
        }
    }
}
