package com.kugou.whaledb.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * Created by Administrator on 2016/4/7 0007.
 */
public class HadoopUtil {
    public static FileSystem getFs(Configuration conf) throws IOException {
        FileSystem fs = null;
        fs = FileSystem.get(conf);
        fs.setConf(conf);
        return fs;
    }

    public static Configuration getConf() {
        return new Configuration();
    }

}
