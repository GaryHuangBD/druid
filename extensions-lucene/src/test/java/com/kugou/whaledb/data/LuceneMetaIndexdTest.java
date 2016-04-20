package com.kugou.whaledb.data;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.LuceneMetaIndexd;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

/**
 * Created by garyhuang on 2016/4/14.
 */
public class LuceneMetaIndexdTest {

    File tmpDir = null;

    @Before
    public void setUp() throws Exception {

        tmpDir = Files.createTempDir();
    }

    @After
    public void tearDown() throws Exception {

        FileUtils.deleteDirectory(tmpDir);
    }

    @Test
    public void write() throws Exception {
        File meta = new File(tmpDir, LuceneMetaIndexd.META_FILE);
        FileOutputStream fileOutputStream = new FileOutputStream(meta);
        FileChannel fileChannel = fileOutputStream.getChannel();
        Map<String, String> metricAndType = Maps.newHashMap();
        metricAndType.put("m1", ValueType.FLOAT.name());
        metricAndType.put("m2", ValueType.LONG.name());
        try {
            LuceneMetaIndexd.write(
                    metricAndType,
                    1460636208000L,
                    1460636211600L,
                    fileChannel
            );
        } finally {
            fileChannel.close();
            fileOutputStream.close();
        }


        ByteBuffer bytedata = LuceneMetaIndexd.readFile(meta);
        Map<String, String> mtricAndType = LuceneMetaIndexd.readMetricAndType(bytedata);
        System.out.println(mtricAndType.toString());
        Assert.assertEquals(1460636208000L, (long) LuceneMetaIndexd.readLong(bytedata));
        Assert.assertEquals(1460636211600L, (long) LuceneMetaIndexd.readLong(bytedata));

        meta.delete();
    }


}