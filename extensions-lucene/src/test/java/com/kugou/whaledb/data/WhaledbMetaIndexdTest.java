package com.kugou.whaledb.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.druid.common.utils.SerializerUtils;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.loading.LuceneQueryableIndexFactory;
import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.junit.Assert.*;

/**
 * Created by garyhuang on 2016/4/14.
 */
public class WhaledbMetaIndexdTest {

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
        File meta = new File(tmpDir, WhaledbMetaIndexd.META_FILE);
        FileOutputStream fileOutputStream = new FileOutputStream(meta);
        FileChannel fileChannel = fileOutputStream.getChannel();
        WhaledbMetaIndexd.write(
                new String[]{"dim1", "dim2"},
                new String[]{"metric1", "metric2", "metric3"},
                1460636208000L,
                1460636211600L,
                EasyMock.createMock(BitmapSerdeFactory.class),
                fileChannel
        );
        fileChannel.close();
        fileOutputStream.close();
        ByteBuffer bytedata = WhaledbMetaIndexd.readFile(new File("data/data2"));
        Assert.assertEquals(2, WhaledbMetaIndexd.readIndexed(bytedata).size());
        Assert.assertEquals(3, WhaledbMetaIndexd.readIndexed(bytedata).size());
        Assert.assertEquals(1460636208000L, (long)WhaledbMetaIndexd.readLong(bytedata));
        Assert.assertEquals(1460636211600L, (long)WhaledbMetaIndexd.readLong(bytedata));
        Assert.assertTrue(WhaledbMetaIndexd.readBitmapSerdeFactory(bytedata) instanceof BitmapSerdeFactory);

    }


}