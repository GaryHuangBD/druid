package io.druid.segment;

import com.google.common.collect.ImmutableMap;
import com.kugou.whaledb.data.WhaledbMetaIndexd;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import io.druid.segment.column.Column;
import io.druid.segment.data.Indexed;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

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
                    .put(99, new LuceneIndexIO.LuceneIndexLoader())
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

//            ByteBuffer indexBuffer = WhaledbMetaIndexd.readFile(new File(inDir, WhaledbMetaIndexd.META_FILE));
//            final Indexed<String> availableMetrics = WhaledbMetaIndexd.readIndexed(indexBuffer);
//            final Interval dataInterval = new Interval(WhaledbMetaIndexd.readLong(indexBuffer), WhaledbMetaIndexd.readLong(indexBuffer));
//            final BitmapFactory bitmapFactory = WhaledbMetaIndexd.readBitmapSerdeFactory(indexBuffer).getBitmapFactory();
            Directory dir = new NIOFSDirectory(Paths.get(inDir.toURI()));
            QueryableIndex index  = new LuceneQueryableIndex(dir, null);
            log.debug("Mapped Lucene index[%s] in %,d millis", inDir, System.currentTimeMillis() - startTime);
            return index;
        }
    }

}
