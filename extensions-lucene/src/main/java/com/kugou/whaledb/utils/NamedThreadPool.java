package com.kugou.whaledb.utils;

import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadPool {
    public static ThreadPool create(int paramInt1, int paramInt2, long paramLong, TimeUnit paramTimeUnit, BlockingQueue<Runnable> paramBlockingQueue, String paramString) {
        return new ThreadPool(Math.max(paramInt1, 1), paramInt2, paramLong, paramTimeUnit, paramBlockingQueue, new NamedThreadFactory(paramString));
    }

    public static ThreadPool createHigh(int paramInt1, int paramInt2, long paramLong, TimeUnit paramTimeUnit, BlockingQueue<Runnable> paramBlockingQueue, String paramString) {
        return new ThreadPool(Math.max(paramInt1, 1), paramInt2, paramLong, paramTimeUnit, paramBlockingQueue, new NamedHighThreadFactory(paramString));
    }

    public static ThreadPool createLow(int paramInt1, int paramInt2, long paramLong, TimeUnit paramTimeUnit, BlockingQueue<Runnable> paramBlockingQueue, String paramString) {
        return new ThreadPool(Math.max(paramInt1, 1), paramInt2, paramLong, paramTimeUnit, paramBlockingQueue, new NamedThreadPool.NamedLowThreadFactory(paramString));
    }

    public static class ThreadPool extends ThreadPoolExecutor {
        private static Logger LOG = Logger.getLogger(ThreadPool.class);
        BlockingQueue<Runnable> Queue;

        public ThreadPool(int paramInt1, int paramInt2, long paramLong, TimeUnit paramTimeUnit, BlockingQueue<Runnable> paramBlockingQueue, ThreadFactory paramThreadFactory) {
            super(paramInt1, paramInt2, 1200L, TimeUnit.SECONDS, paramBlockingQueue, paramThreadFactory);
            this.Queue = paramBlockingQueue;
        }

        public void execute(final Runnable runnable) {
            if ((runnable instanceof RunWithName)) {
                executeWithname((RunWithName) runnable);
                return;
            }
            final long ts = System.currentTimeMillis();
            super.execute(new Runnable() {
                @Override
                public void run() {
                    long l = System.currentTimeMillis() - ts;
                    if (l > 10000L) {
                        LOG.info("ThreadPoolBusy:" + runnable.getClass().getName() + ",diff:" + l);
                    }
                    runnable.run();
                }
            });
        }

        private void executeWithname(final RunWithName runWithName) {
            if (Queue.size() > runWithName.MaxQueue()) {
                LOG.info("ThreadPoolBusy Drop :" + runWithName.getClass().getName() + "," + runWithName.debugmsg() + ",qsize:" + Queue.size());
                return;
            }
            final long ts = System.currentTimeMillis();
            super.execute(new Runnable() {
                @Override
                public void run() {
                    long l = System.currentTimeMillis() - ts;
                    if (l > 10000L) {
                        LOG.info("ThreadPoolBusy:" + runWithName.getClass().getName() + "," + runWithName.debugmsg() + ",diff:" + l);
                    }
                    try {
                        runWithName.run();
                    } catch (Throwable localThrowable) {
                        LOG.error("ThreadPoolERROR:" + runWithName.getClass().getName() + "," + runWithName.debugmsg() + ",diff:" + l, localThrowable);
                    }
                }
            });
        }
    }

    public static class NamedThreadFactory
            implements ThreadFactory {
        static final AtomicInteger poolNumber = new AtomicInteger(1);
        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        NamedThreadFactory(String paramString) {
            SecurityManager localSecurityManager = System.getSecurityManager();
            this.group = (localSecurityManager != null ? localSecurityManager.getThreadGroup() : Thread.currentThread().getThreadGroup());
            this.namePrefix = ("pool-" + paramString + "-" + poolNumber.getAndIncrement() + "-thread-");
        }

        public Thread newThread(Runnable paramRunnable) {
            Thread localThread = new Thread(this.group, paramRunnable, this.namePrefix + this.threadNumber.getAndIncrement(), 0L);
            if (localThread.isDaemon())
                localThread.setDaemon(false);
            if (localThread.getPriority() != 5)
                localThread.setPriority(5);
            return localThread;
        }
    }

    public static class NamedHighThreadFactory implements ThreadFactory {
        static final AtomicInteger poolNumber = new AtomicInteger(1);
        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        NamedHighThreadFactory(String paramString) {
            SecurityManager localSecurityManager = System.getSecurityManager();
            this.group = (localSecurityManager != null ? localSecurityManager.getThreadGroup() : Thread.currentThread().getThreadGroup());
            this.namePrefix = ("pool-" + paramString + "-" + poolNumber.getAndIncrement() + "-thread-");
        }

        public Thread newThread(Runnable paramRunnable) {
            Thread localThread = new Thread(this.group, paramRunnable, this.namePrefix + this.threadNumber.getAndIncrement(), 0L);
            if (localThread.isDaemon())
                localThread.setDaemon(false);
            if (localThread.getPriority() != 10)
                localThread.setPriority(10);
            return localThread;
        }
    }

    public static class NamedLowThreadFactory
            implements ThreadFactory {
        static final AtomicInteger poolNumber = new AtomicInteger(1);
        final ThreadGroup group;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final String namePrefix;

        NamedLowThreadFactory(String paramString) {
            SecurityManager localSecurityManager = System.getSecurityManager();
            this.group = (localSecurityManager != null ? localSecurityManager.getThreadGroup() : Thread.currentThread().getThreadGroup());
            this.namePrefix = ("pool-" + paramString + "-" + poolNumber.getAndIncrement() + "-thread-");
        }

        public Thread newThread(Runnable paramRunnable) {
            Thread localThread = new Thread(this.group, paramRunnable, this.namePrefix + this.threadNumber.getAndIncrement(), 0L);
            if (localThread.isDaemon())
                localThread.setDaemon(false);
            if (localThread.getPriority() != 1)
                localThread.setPriority(1);
            return localThread;
        }
    }

    public static abstract class RunWithName
            implements Runnable {
        public abstract String debugmsg();

        public int MaxQueue() {
            return 100000000;
        }

        public abstract void run();
    }
}
