package io.druid.segment;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.BitmapFactory;
import io.druid.segment.column.*;
import io.druid.segment.data.ArrayIndexed;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedLongs;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;
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
            dimSet.add(dim);
            FieldInfo fieldInfo = getFieldInfo(indexReader, dim);
            if (fieldInfo.getDocValuesType() == DocValuesType.SORTED) {
                columns.put(dim,
                            new TermsColumn(
                                        fields.terms(dim),
                                        MultiDocValues.getSortedValues(indexReader, dim)
                            )
                );
            } else if (fieldInfo.getDocValuesType() == DocValuesType.SORTED_SET) {
                columns.put(dim,
                            new TermsColumn(
                                        fields.terms(dim),
                                        MultiDocValues.getSortedSetValues(indexReader, dim)
                            )
                );
            }
        }
        String[] dims = dimSet.toArray(new String[dimSet.size()]);
        availableDimensions = new ArrayIndexed<>(dims, String.class);
        // TODO: add metric column ?
        columnNames = availableDimensions;
        bitmapFactory = new LuceneBitmapFactory();
    }


    public FieldInfo getFieldInfo(IndexReader indexReader, String dim) {
        final List<LeafReaderContext> leaves = indexReader.leaves();
        final int size = leaves.size();
        if (size == 0) {
            return null;
        } else if (size == 1) {
            return leaves.get(0).reader().getFieldInfos().fieldInfo(dim);
        }
        FieldInfo fieldInfo = null;
        for (int i = 0; i < size; i++) {
            fieldInfo = leaves.get(i).reader().getFieldInfos().fieldInfo(dim);
            if (fieldInfo != null) {
                return fieldInfo;
            }
        }
        return fieldInfo;
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


        public TermsColumn(Terms terms, SortedDocValues docValues) throws IOException {
            this.length = Ints.checkedCast(terms.getDocCount());
            dictionaryEncodedColumn = new LuceneDictionaryEncodedColumn(length, docValues);
            bitmapIndex = new LuceneBitmapIndex(terms.iterator(), dictionaryEncodedColumn, bitmapFactory);
        }

        public TermsColumn(Terms terms, SortedSetDocValues docValues) throws IOException {
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
        public GenericColumn getGenericColumn() {
            return new IndexedLongsGenericColumn(new IndexedLongs() {
                @Override
                public int size() {
                    return length;
                }

                @Override
                public long get(int index) {

                    return Long.parseLong(dictionaryEncodedColumn.lookupName(index));
                }

                @Override
                public void fill(int index, long[] toFill) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int binarySearch(long key) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int binarySearch(long key, int from, int to) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void close() throws IOException {

                }
            });
        }

        private final ColumnCapabilitiesImpl CAPABILITIES = new ColumnCapabilitiesImpl()
                .setType(ValueType.STRING);
        @Override
        public ColumnCapabilities getCapabilities() {
            return CAPABILITIES;
        }

        @Override
        public int getLength() {
            return length;
        }
    }
}
