package io.druid.segment;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.BitmapFactory;
import io.druid.segment.column.*;
import io.druid.segment.data.ArrayIndexed;
import io.druid.segment.data.Indexed;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class LuceneQueryableIndex implements QueryableIndex {
    private final  Interval dataInterval;
    private final Map<String, String> metricAndType;
    private final  Indexed<String> columnNames;
    private final Indexed<String> availableDimensions;
    private final BitmapFactory bitmapFactory;
    private Map<String, Column> columns;
    private Map<String, Object> metadata;

    private final Directory directory;
    private final IndexReader indexReader;
    private final Fields fields;
    private final int length;

    public LuceneQueryableIndex(
        Directory directory,
        Interval dataInterval,
        Map<String, String> metricAndType,
        Map<String, Object> metadata
    ) throws IOException {
        this.directory = directory;
        this.dataInterval = dataInterval;
        this.metricAndType = metricAndType;
        this.metadata = metadata;
        indexReader = DirectoryReader.open(directory);

        fields = MultiFields.getFields(indexReader);
        long size = fields.terms(Column.TIME_COLUMN_NAME).getDocCount();
        length = Ints.checkedCast(size);
        columns = Maps.newHashMap();
        Set<String> dimSet = Sets.newTreeSet();
        for (String dim : fields) {
            if (!Column.TIME_COLUMN_NAME.equals(dim)){
                dimSet.add(dim);
                columns.put(dim, new TermsColumn(fields.terms(dim), MultiDocValues.getSortedValues(indexReader, dim))) ;
            }
        }
        String[] dims = dimSet.toArray(new String[dimSet.size()]);
        availableDimensions = new ArrayIndexed<>(dims, String.class);
        // TODO: add metric column
        columnNames = availableDimensions;
        bitmapFactory = new LuceneBitmapFactory();
    }

    @Override
    public Interval getDataInterval()
    {
        return dataInterval;
    }

    @Override
    public int getNumRows()
    {
        return length;
    }

    @Override
    public Indexed<String> getColumnNames()
    {
        return columnNames;
    }

    @Override
    public Indexed<String> getAvailableDimensions()
    {
        return availableDimensions;
    }

    @Override
    public BitmapFactory getBitmapFactoryForDimensions()
    {
        return bitmapFactory;
    }

    @Override
    public Column getColumn(String columnName)
    {
        return columns.get(columnName);
    }

    @Override
    public void close() throws IOException
    {
        indexReader.close();
        directory.close();
    }

    @Override
    public Map<String, Object> getMetaData()
    {
        return metadata;
    }

    public class TermsColumn extends AbstractColumn {
        private final int length;
        private final DictionaryEncodedColumn dictionaryEncodedColumn;
        private final BitmapIndex bitmapIndex;

//        public TermsColumn(Terms terms, SortedSetDocValues docValues) throws IOException {
//            this.terms = terms;
//            length = Ints.checkedCast(terms.size());
//            dictionaryEncodedColumn = new LuceneDictionaryEncodedColumn(terms);
//        }

        public TermsColumn(Terms terms, SortedDocValues docValues) throws IOException {
            this.length = Ints.checkedCast(terms.getDocCount());
            dictionaryEncodedColumn = new LuceneDictionaryEncodedColumn(length, docValues);
            bitmapIndex = new LuceneBitmapIndex(terms.iterator(), dictionaryEncodedColumn, bitmapFactory);
        }

        @Override
        public DictionaryEncodedColumn getDictionaryEncoding()
        {
            return dictionaryEncodedColumn;
        }

        @Override
        public BitmapIndex getBitmapIndex() {
            return bitmapIndex;
        }

        @Override
        public int getLength() {
            return length;
        }
    }
}
