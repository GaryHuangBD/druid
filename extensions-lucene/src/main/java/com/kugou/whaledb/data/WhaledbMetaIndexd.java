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
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.Indexed;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * Created by garyhuang on 2016/4/14.
 */
public class WhaledbMetaIndexd {

    public static final String META_FILE = "meta";

    private static final ObjectMapper mapper;

    static {
        final Injector injector = GuiceInjectors.makeStartupInjectorWithModules(
                ImmutableList.<Module>of(
                        new Module()
                        {
                            @Override
                            public void configure(Binder binder)
                            {
                                JsonConfigProvider.bind(binder, "druid.processing.bitmap", BitmapSerdeFactory.class);
                            }
                        }
                )
        );
        mapper = injector.getInstance(ObjectMapper.class);
    }

    private static final SerializerUtils serializerUtils = new SerializerUtils();

    public static void write(String[] array, FileChannel channel) throws IOException {
        write(Arrays.asList(array), channel);
    }

    public static void write(Iterable<String> objectsIterable, FileChannel channel) throws IOException {
        GenericIndexed.fromIterable(objectsIterable, GenericIndexed.STRING_STRATEGY).writeToChannel(channel);
    }
    public static void write(long num, FileChannel channel) throws IOException {
        serializerUtils.writeLong(channel, num);
    }

    public static void write(BitmapSerdeFactory bitmapSerdeFactory, FileChannel channel) throws IOException {
        serializerUtils.writeString(channel, mapper.writeValueAsString(bitmapSerdeFactory));
    }


    public static ByteBuffer readFile(File file) throws IOException {
        return Files.map(file, FileChannel.MapMode.READ_ONLY);
    }

    public static Indexed<String> readIndexed(ByteBuffer indexBuffer) {
        return  GenericIndexed.read(indexBuffer, GenericIndexed.STRING_STRATEGY);
    }

    public static Long readLong(ByteBuffer indexBuffer) {
        return  indexBuffer.getLong();
    }

    public static BitmapSerdeFactory readBitmapSerdeFactory(ByteBuffer indexBuffer) throws IOException {
        return mapper.readValue(
                serializerUtils.readString(indexBuffer),
                BitmapSerdeFactory.class
        );
    }

    public static void write(String[] dimensions, String[] metrics, long minTime, long maxTime,
                             BitmapSerdeFactory bitmapSerdeFactory, FileChannel channel) throws IOException {
        write(dimensions, channel);
        write(metrics, channel);
        write(minTime, channel);
        write(maxTime, channel);
        write(bitmapSerdeFactory, channel);
    }

}
