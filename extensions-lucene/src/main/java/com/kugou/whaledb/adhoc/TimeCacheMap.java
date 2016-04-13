package com.kugou.whaledb.adhoc;


import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.kugou.whaledb.utils.PoolTask;
import com.kugou.whaledb.utils.TimerPools;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

public class TimeCacheMap<K, V>
        implements Serializable {
    private static final long serialVersionUID = 1L;

    public static interface ExpiredCallback<K, V> {
        public void expire(K key, V val);

        public void commit();
    }

    private static final int f = 3;
    private static Logger LOG = Logger.getLogger(TimeCacheMap.class);
    private LinkedList<Map<K, V>> _buckets;
    private ExpiredCallback<K, V> _callback;
    private final Object _lock = new Object();
    private Thread _cleaner = null;
    PoolTask task = null;
    TimerPools timerPools = null;

    private int numBuckets;
    private long lasttime = System.currentTimeMillis();
    private long localMergerDelay = 20000L;

    public static class CleanExecute<K, V> {
        public void executeClean(TimeCacheMap<K, V> t) {
            t.clean();
        }
    }

    CleanExecute<K, V> _cleanlock = new CleanExecute<K, V>();

    private Map<K, V> createMap() {
        if (lursize > 102400000) {
            return new HashMap();
        }
        return new ConcurrentLinkedHashMap.Builder().maximumWeightedCapacity(lursize).listener(new EvictionListener<K, V>() {

            @Override
            public void onEviction(K k, V v) {
                LOG.info("expire " + v);
                _callback.expire(k, v);
                _callback.commit();
            }
        }).build();
    }

    private int lursize = Integer.MAX_VALUE;

    public TimeCacheMap(int lursize, TimerPools timerPools, int expirationSecs, int numBuckets, ExpiredCallback<K, V> paramjo, CleanExecute<K, V> cleanlock) {
        this.lursize = lursize;
        if (cleanlock != null) {
            this._cleanlock = cleanlock;
        }
        this.numBuckets = numBuckets;
        if (numBuckets < 2) {
            throw new IllegalArgumentException("numBuckets must be >= 2");
        }
        _buckets = new LinkedList<Map<K, V>>();
        for (int i1 = 0; i1 < numBuckets; i1++) {
            _buckets.add(createMap());
        }
        _callback = paramjo;
        final long expirationMillis = expirationSecs * 1000L;
        final long sleepTime = expirationMillis / (numBuckets - 1);
        localMergerDelay = (sleepTime - 100L);
        if (timerPools == null) {
            _cleaner = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        LOG.info("_cleaner start");
                        try {
                            Thread.currentThread().sleep(sleepTime);
                            maybeClean();
                        } catch (Throwable ex) {
                            LOG.error("_cleaner", ex);
                        }
                    }
                }
            });
            _cleaner.setDaemon(true);
            _cleaner.start();
        } else {
            this.task = new PoolTask() {
                @Override
                public void execute() {
                    maybeClean();
                }
            };
            timerPools.schedule(task, sleepTime, sleepTime);
            this.timerPools = timerPools;
        }
    }

    private void timerReset() {
        lasttime = System.currentTimeMillis();
    }

    private boolean isTimeout() {
        long l1 = System.currentTimeMillis();
        if (lasttime + localMergerDelay <= l1) {
            return true;
        }
        return false;
    }

    public void maybeClean() {
        synchronized (_lock) {
            if (!isTimeout()) {
                return;
            }
            timerReset();
        }
        _cleanlock.executeClean(this);
    }


    private void clean() {
        try {
            Map<K, V> dead = null;
            synchronized (_lock) {
                dead = _buckets.removeLast();
                _buckets.addFirst(createMap());
            }

            if (_callback != null) {
                synchronized (_callback) {
                    for (Entry<K, V> entry : dead.entrySet()) {
                        _callback.expire(entry.getKey(), entry.getValue());
                    }
                    _callback.commit();
                }
            }

        } catch (Throwable localThrowable) {
            LOG.error("_cleaner maybeClean", localThrowable);
        }
    }


    public void forceClean() {
        synchronized (_lock) {
            timerReset();
        }
        clean();
    }

    public void forceTimeout(Timeout<K, V> fetch, Update<K, V> d) {
        LinkedList<Map<K, V>> lastdata = null;
        synchronized (_lock) {
            lastdata = _buckets;
            _buckets = new LinkedList();
            for (int i = 0; i < numBuckets; i++) {
                _buckets.add(createMap());
            }
        }

        if ((_callback != null) && (lastdata != null)) {
            synchronized (_callback) {
                HashMap<K, V> needupdate = new HashMap<>();
                for (Map<K, V> dead : lastdata) {
                    for (Entry<K, V> entry : dead.entrySet()) {
                        K key = entry.getKey();
                        V value = entry.getValue();
                        if (fetch.timeout(key, value)) {
                            _callback.expire(key, value);
                        } else {
                            needupdate.put(key, value);
                        }
                    }
                }
                _callback.commit();
                if (needupdate.size() > 0) {
                    updateAll(needupdate, d);
                }
            }
        }
    }


    public void forceTimeout() {
        LinkedList<Map<K, V>> lastdata = null;
        synchronized (_lock) {
            lastdata = _buckets;
            _buckets = new LinkedList<>();
            for (int i1 = 0; i1 < numBuckets; i1++) {
                _buckets.add(createMap());
            }
        }

        if ((_callback != null) && (lastdata != null)) {
            synchronized (_callback) {
                for (Map<K, V> dead : lastdata) {
                    for (Entry<K, V> entry : dead.entrySet()) {
                        _callback.expire(entry.getKey(), entry.getValue());
                    }
                }
                _callback.commit();
            }
        }
    }


//    public void d() {
//        synchronized (_lock) {
//            _buckets = new LinkedList();
//            for (int i1 = 0; i1 < numBuckets; i1++) {
//                _buckets.add(createMap());
//            }
//        }
//    }


    public TimeCacheMap(TimerPools paramTimerPools, int paramInt, ExpiredCallback<K, V> paramjo) {
        this(Integer.MAX_VALUE, paramTimerPools, paramInt, 3, paramjo, null);
    }

    public TimeCacheMap(TimerPools paramTimerPools, int paramInt, ExpiredCallback<K, V> paramjo, CleanExecute<K, V> paramjm) {
        this(Integer.MAX_VALUE, paramTimerPools, paramInt, 3, paramjo, paramjm);
    }

    public TimeCacheMap(int paramInt1, TimerPools paramTimerPools, int paramInt2, ExpiredCallback<K, V> paramjo, CleanExecute<K, V> paramjm) {
        this(paramInt1, paramTimerPools, paramInt2, 3, paramjo, paramjm);
    }

    public boolean containsKey(K key) {
        synchronized (_lock) {
            for (Map<K, V> bucket : _buckets) {
                if (bucket.containsKey(key)) {
                    return true;
                }
            }
            return false;
        }
    }


//    public void a(jr<K, V> paramjr) {
//        synchronized (_lock) {
//            for (Map<K, V> localMap : _buckets) {
//                for (Entry<K, V> localEntry : localMap.entrySet()) {
//                    paramjr.a(localEntry.getKey(), localEntry.getValue());
//                }
//            }
//        }
//    }


//   /* Error */
//
//    public V b(K paramK) {
//        // Byte code:
//        //   0: aload_0
//        //   1: getfield 45	cn/net/ycloudpg/ji:i	Ljava/lang/Object;
//        //   4: dup
//        //   5: astore_2
//        //   6: monitorenter
//        //   7: aload_0
//        //   8: aload_1
//        //   9: invokespecial 58	cn/net/ycloudpg/ji:e	(Ljava/lang/Object;)Ljava/lang/Object;
//        //   12: aload_2
//        //   13: monitorexit
//        //   14: areturn
//        //   15: astore_3
//        //   16: aload_2
//        //   17: monitorexit
//        //   18: aload_3
//        //   19: athrow
//        // Line number table:
//        //   Java source line #373	-> byte code offset #0
//        //   Java source line #374	-> byte code offset #7
//        //   Java source line #375	-> byte code offset #15
//        // Local variable table:
//        //   start	length	slot	name	signature
//        //   0	20	0	this	ji
//        //   0	20	1	paramK	K
//        //   5	12	2	Ljava/lang/Object;	Object
//        //   15	4	3	localObject1	Object
//        // Exception table:
//        //   from	to	target	type
//        //   7	14	15	finally
//        //   15	18	15	finally
//
//    }
//
//
    public V get(K paramK) {
        synchronized (_lock) {
            V value = getNolock(paramK);
            if (value != null) {
                putNolock(paramK, value);
            }
            return value;
        }
    }


    private V getNolock(K key) {
        for (Map<K, V> localMap : _buckets) {
            if (localMap.containsKey(key)) {
                return localMap.get(key);
            }
        }
        return null;
    }

    private void putNolock(K paramK, V paramV) {
        Iterator localIterator = _buckets.iterator();
        Map localMap = (Map) localIterator.next();
        localMap.put(paramK, paramV);
        while (localIterator.hasNext()) {
            localMap = (Map) localIterator.next();
            localMap.remove(paramK);
        }

    }

    public void put(K key, V value) {
        synchronized (_lock) {
            this.putNolock(key, value);
        }
    }

    public static interface Update<K, V> {
        public V update(K key, V old, V newval);
    }

    public static interface Timeout<K, V> {
        public boolean timeout(K key, V val);
    }

    public void updateAll(Map<K, V> bucket, Update<K, V> d) {
        synchronized (_lock) {
            for (Entry<K, V> e : bucket.entrySet()) {
                K key = e.getKey();
                V old = getNolock(key);
                V newval = e.getValue();
                V finalVal = d.update(key, old, newval);
                putNolock(key, finalVal);
            }
        }
    }


    public void update(K key, V newval, Update<K, V> d) {
        synchronized (_lock) {
            V old = this.getNolock(key);
            V finalVal = d.update(key, old, newval);
            this.putNolock(key, finalVal);
        }
    }


    public V remove(K key) {
        synchronized (_lock) {
            for (Map<K, V> bucket : _buckets) {
                if (bucket.containsKey(key)) {
                    return bucket.remove(key);
                }
            }
            return null;
        }
    }

    public int size() {
        synchronized (_lock) {
            int size = 0;
            for (Map<K, V> bucket : _buckets) {
                size += bucket.size();
            }
            return size;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            if (_cleaner != null) {
                _cleaner.interrupt();
            }

            if (timerPools != null) {
                task.cancel();
                timerPools.purge();
            }
        } finally {
            super.finalize();
        }
    }

}