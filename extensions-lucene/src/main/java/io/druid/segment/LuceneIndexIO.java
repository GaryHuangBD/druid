package io.druid.segment;

import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import io.druid.common.utils.JodaUtils;
import io.druid.segment.data.LuceneMetaIndexd;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Created by garyhuang on 2016/4/13.
 * 1.add LuceneIndexIO, add version (10) in file version.bin
 * 2.add meta file, contains availableDimensions,availableMetrics
 */
public class LuceneIndexIO {

    private static final EmittingLogger log = new EmittingLogger(LuceneIndexIO.class);

    public static final byte LUCENE_VERSION = 0x7f;

    private static final Map<Integer, IndexIO.IndexLoader> indexLoaders =
            ImmutableMap.<Integer, IndexIO.IndexLoader>builder()
                    .put(0, new IndexIO.LegacyIndexLoader())
                    .put(1, new IndexIO.LegacyIndexLoader())
                    .put(2, new IndexIO.LegacyIndexLoader())
                    .put(3, new IndexIO.LegacyIndexLoader())
                    .put(4, new IndexIO.LegacyIndexLoader())
                    .put(5, new IndexIO.LegacyIndexLoader())
                    .put(6, new IndexIO.LegacyIndexLoader())
                    .put(7, new IndexIO.LegacyIndexLoader())
                    .put(8, new IndexIO.LegacyIndexLoader())
                    .put(9, new IndexIO.V9IndexLoader())
                    .put(127, new LuceneIndexIO.LuceneIndexLoader())
                    .build();

    public static QueryableIndex loadIndex(File inDir) throws IOException {
        final int version = 99;
        //TODO indexing write file "version.bin"
//        final int version = SegmentUtils.getVersionFromDir(inDir);
        final IndexIO.IndexLoader loader = indexLoaders.get(version);

        if (loader != null) {
            return loader.load(inDir);
        } else {
            throw new ISE("Unknown index version[%s]", version);
        }
    }

    static class LuceneIndexLoader implements IndexIO.IndexLoader {

        @Override
        public QueryableIndex load(File inDir) throws IOException {
            log.debug("Mapping Lucene index[%s]", inDir);
            long startTime = System.currentTimeMillis();

            File metaFile = new File(inDir, LuceneMetaIndexd.META_FILE);
            Map<String, String> metricAndType = Maps.newHashMap();
            Interval dataInterval = new Interval(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT);

            if (metaFile.exists()) {
                ByteBuffer indexBuffer = LuceneMetaIndexd.readFile(new File(inDir, LuceneMetaIndexd.META_FILE));
                metricAndType = LuceneMetaIndexd.readMetricAndType(indexBuffer);
                dataInterval = new Interval(LuceneMetaIndexd.readLong(indexBuffer), LuceneMetaIndexd.readLong(indexBuffer));
            }

            Directory dir = new NIOFSDirectory(Paths.get(inDir.toURI()));
            QueryableIndex index  = new LuceneQueryableIndex(dir, dataInterval, metricAndType, null);
            log.debug("Mapped Lucene index[%s] in %,d millis", inDir, System.currentTimeMillis() - startTime);
            return index;
        }
    }

}
