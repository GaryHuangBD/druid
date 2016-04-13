package com.kugou.whaledb.utils;

import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with whaledb.
 * User: chiyao
 * Date: 2016/4/7
 * Time: 17:47
 * Description:
 */
public class UniqConfig {
    private static AtomicReference<UniqConfig> INSTANCEREF = new AtomicReference(null);

    private Configuration confcache;
    private Object lock;

    public static synchronized UniqConfig INSTANCE() {
        UniqConfig instance = INSTANCEREF.get();
        if (instance == null) {
            instance = new UniqConfig();
            INSTANCEREF.set(instance);
        }
        return instance;
    }

    public int RealTimeDoclistBuffer() {
        return 1024;
    }

    public int RealTimeMaxIndexCountBuffer() {
        return 128;
    }

    public int RealTimeMergeFactorBuffer() {
        return 32;
    }

    public boolean isUseCompoundfileRam2disk() {
        return true;
    }

    public static Integer getMaxMergerShard()
    {
        return 64;
    }

    public static String GroupJoinString()
    {
        return "@";
    }

    public static String GroupJoinTagString()
    {
        return "m";
    }

    public static Integer defaultCrossMaxLimit()
    {
        return 10010;
    }

    public Configuration getConfCache() {
        synchronized (lock) {
            if (confcache == null) {
                confcache = HadoopUtil.getConf();
            }
            return confcache;
        }
    }


}
