package io.druid.segment.data;

import com.google.api.client.util.Maps;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.io.Files;
import io.druid.common.utils.SerializerUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by garyhuang on 2016/4/14.
 * whaleDB common info
 */
public class LuceneMetaIndexd {

    public static final String META_FILE = "meta";

    private static final SerializerUtils serializerUtils = new SerializerUtils();

    public static void writeMetricAndType(Map<String, String> metricAndType, FileChannel channel) throws IOException {
        if (metricAndType == null) {
            throw new IOException("metricAndType cannot be null");
        }
        String metricAndTypeString ="";
        if (!metricAndType.isEmpty()) {
            metricAndTypeString = Joiner.on(",").withKeyValueSeparator("=").join(metricAndType);
        }
        serializerUtils.writeString(channel, metricAndTypeString);
    }


    public static void write(String[] array, FileChannel channel) throws IOException {
        write(Arrays.asList(array), channel);
    }

    public static void write(Iterable<String> objectsIterable, FileChannel channel) throws IOException {
        GenericIndexed.fromIterable(objectsIterable, GenericIndexed.STRING_STRATEGY).writeToChannel(channel);
    }

    public static void write(long num, FileChannel channel) throws IOException {
        serializerUtils.writeLong(channel, num);
    }


    public static ByteBuffer readFile(File file) throws IOException {
        return Files.map(file, FileChannel.MapMode.READ_ONLY);
    }

    public static Map<String, String> readMetricAndType(ByteBuffer indexBuffer) throws IOException {
        String metricAndTypeString = serializerUtils.readString(indexBuffer);
        if (StringUtils.isBlank(metricAndTypeString)) {
            return Maps.newHashMap();
        }
        return Splitter.on(",").withKeyValueSeparator("=").split(metricAndTypeString);
    }

    public static Long readLong(ByteBuffer indexBuffer) {
        return  indexBuffer.getLong();
    }


    public static void write(Map<String, String> metricAndType, long minTime, long maxTime, FileChannel channel) throws IOException {
        writeMetricAndType(metricAndType, channel);
        write(minTime, channel);
        write(maxTime, channel);
    }

}
