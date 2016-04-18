package io.druid.segment;

import com.google.common.collect.ImmutableMap;
import com.kugou.whaledb.data.WhaledbMetaIndexd;
import com.kugou.whaledb.hdfsDirectory.FileSystemDirectory;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.segment.column.Column;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.Indexed;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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
        final int version = SegmentUtils.getVersionFromDir(inDir);

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
            Indexed<String> columnNames = null;
            Map<String, Column> columns = null;


            ByteBuffer indexBuffer = WhaledbMetaIndexd.readFile(new File(inDir, WhaledbMetaIndexd.META_FILE));
            final Indexed<String> availableDimensions = WhaledbMetaIndexd.readIndexed(indexBuffer);
            final Indexed<String> availableMetrics = WhaledbMetaIndexd.readIndexed(indexBuffer);
            final Interval dataInterval = new Interval(WhaledbMetaIndexd.readLong(indexBuffer), WhaledbMetaIndexd.readLong(indexBuffer));
            final BitmapFactory bitmapFactory = WhaledbMetaIndexd.readBitmapSerdeFactory(indexBuffer).getBitmapFactory();

            Configuration conf = new Configuration();
            //TODO here just for test
            FileSystem fs = new NoPermissionFileSystem();
            fs.initialize(FileSystem.getDefaultUri(conf), conf);
            Directory dir = new FileSystemDirectory(new Path(inDir.getPath()), new SingleInstanceLockFactory(), conf, fs);
            IndexReader reader = DirectoryReader.open(dir);
            Fields fields = MultiFields.getFields(reader);

            return new LuceneQueryableIndex(
                    dataInterval,
                    columnNames,
                    availableDimensions,
                    bitmapFactory,
                    columns,
                    null
            );
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
