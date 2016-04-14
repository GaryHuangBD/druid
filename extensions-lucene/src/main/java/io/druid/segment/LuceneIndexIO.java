package io.druid.segment;

import com.google.common.collect.ImmutableMap;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.segment.column.Column;
import io.druid.segment.data.Indexed;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
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
            Interval dataInterval = null;
            Indexed<String> columnNames = null;
            Indexed<String> availableDimensions = null;
            BitmapFactory bitmapFactory = null;
            Map<String, Column> columns = null;
            Map<String, Object> metadata = null;

            return new LuceneQueryableIndex(
                    dataInterval,
                    columnNames,
                    availableDimensions,
                    bitmapFactory,
                    columns,
                    metadata
            );
        }
    }

}
