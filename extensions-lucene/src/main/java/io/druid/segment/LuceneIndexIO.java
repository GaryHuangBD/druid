package io.druid.segment;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.ISE;
import io.druid.segment.column.Column;
import io.druid.segment.data.Indexed;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Created by garyhuang on 2016/4/13.
 * 1.add LuceneIndexIO, add version (10) in file version.bin
 * 2.add meta file, contains availableDimensions,availableMetrics
 */
public class LuceneIndexIO {

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
                    .put(10, new LuceneIndexIO.LuceneIndexLoader())
                    .build();

    public static QueryableIndex loadIndex(File inDir) throws IOException {
//        final int version = SegmentUtils.getVersionFromDir(inDir);
//        final IndexIO.IndexLoader loader = indexLoaders.get(version);
        final int version = 10;
        final IndexIO.IndexLoader loader = new LuceneIndexLoader();

        if (loader != null) {
            return loader.load(inDir);
        } else {
            throw new ISE("Unknown index version[%s]", version);
        }
    }

    static class LuceneIndexLoader implements IndexIO.IndexLoader {

        @Override
        public QueryableIndex load(File inDir) throws IOException {
            Indexed<String> columnNames = null;
            Map<String, Column> columns = null;


//            ByteBuffer indexBuffer = WhaledbMetaIndexd.readFile(new File(inDir, WhaledbMetaIndexd.META_FILE));
//            final Indexed<String> availableDimensions = WhaledbMetaIndexd.readIndexed(indexBuffer);
//            final Indexed<String> availableMetrics = WhaledbMetaIndexd.readIndexed(indexBuffer);
//            final Interval dataInterval = new Interval(WhaledbMetaIndexd.readLong(indexBuffer), WhaledbMetaIndexd.readLong(indexBuffer));
//            final BitmapFactory bitmapFactory = WhaledbMetaIndexd.readBitmapSerdeFactory(indexBuffer).getBitmapFactory();

            Directory dir = new NIOFSDirectory(Paths.get(inDir.toURI()));
            return new LuceneQueryableIndex(dir, null);
        }
    }
    static class NoPermissionFileSystem extends LocalFileSystem {
        public FSDataOutputStream create(Path f,
                                         boolean overwrite,
                                         int bufferSize
        ) throws IOException {
            return create(f, null, overwrite, bufferSize,
                    getDefaultReplication(f), getDefaultBlockSize(f), null);
        }
    }

}
