package com.kugou.whaledb.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.druid.common.utils.SerializerUtils;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.loading.LuceneQueryableIndexFactory;
import org.easymock.EasyMock;
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


    @Test
    public void write() throws Exception {
        FileOutputStream fileOutputStream = new FileOutputStream("data/data2");
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

    @Test
    public void write1() throws Exception {

    }

    @Test
    public void write2() throws Exception {

    }

    @Test
    public void write3() throws Exception {

    }

    @Test
    public void readFile() throws Exception {

    }

    @Test
    public void readIndexed() throws Exception {

    }

    @Test
    public void readLong() throws Exception {

    }

    @Test
    public void write4() throws Exception {

    }

}