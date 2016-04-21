package io.druid.segment.column;

import io.druid.segment.data.IndexedInts;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 *
 */
public class LuceneDictionaryEncodedColumn implements DictionaryEncodedColumn {
    private final int length;
    private final SortedDocValues docValues;
    private final SortedSetDocValues multiDocValues;

    public LuceneDictionaryEncodedColumn(int length, SortedDocValues docValues) {
        this.docValues = docValues;
        this.length = length;
        multiDocValues = null;
    }

    public LuceneDictionaryEncodedColumn(int length, SortedSetDocValues multiDocValues) {
        this.multiDocValues = multiDocValues;
        this.length = length;
        docValues = null;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public boolean hasMultipleValues() {
        return docValues == null;
    }

    @Override
    public int getSingleValueRow(int rowNum) {
        return docValues.getOrd(rowNum);
    }

    @Override
    public IndexedInts getMultiValueRow(int rowNum) {
        return null;
    }

    @Override
    public String lookupName(int id) {
        return docValues.get(id).utf8ToString();
    }

    @Override
    public int lookupId(String name) {
        return docValues.lookupTerm(new BytesRef(name));
    }

    @Override
    public int getCardinality() {
        return docValues.getValueCount();
    }

    @Override
    public void close() throws IOException {
    }
}
