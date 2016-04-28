package io.druid.segment.column;

import com.google.api.client.util.Lists;
import com.google.api.client.util.StringUtils;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.druid.segment.data.IndexedInts;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

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
        this.multiDocValues=null;
    }

    public LuceneDictionaryEncodedColumn(int length, SortedSetDocValues multiDocValues) {
        this.length = length;
        this.docValues = null;
        this.multiDocValues = multiDocValues;
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public boolean hasMultipleValues() {
        return (multiDocValues != null);
    }

    @Override
    public int getSingleValueRow(int rowNum) {
        return docValues.getOrd(rowNum);
    }

    @Override
    public IndexedInts getMultiValueRow(int rowNum) {
        multiDocValues.setDocument(rowNum);
        int ord = Ints.checkedCast(multiDocValues.nextOrd());
        final List<Integer> ordList = Lists.newArrayList();
        while (ord != SortedSetDocValues.NO_MORE_ORDS) {
            ordList.add(ord);
            ord = Ints.checkedCast(multiDocValues.nextOrd());
        }


        return new IndexedInts() {
            @Override
            public Iterator<Integer> iterator() {
                return ordList.iterator();
            }

            @Override
            public void close() throws IOException {

            }

            @Override
            public int size() {
                return ordList.size();
            }

            @Override
            public int get(int index) {
                return ordList.get(index);
            }

            @Override
            public void fill(int index, int[] toFill) {
                throw new UnsupportedOperationException();
            }
        };

    }

    @Override
    public String lookupName(int id) {
        if (hasMultipleValues()) {
            return multiDocValues.lookupOrd(id).utf8ToString();
        }else {
            return docValues.lookupOrd(id).utf8ToString();
        }
    }

    @Override
    public int lookupId(String name) {
        if (hasMultipleValues()) {
            return Ints.checkedCast(multiDocValues.lookupTerm(new BytesRef(name)));
        }else {
            return docValues.lookupTerm(new BytesRef(name));
        }
    }

    @Override
    public int getCardinality() {
        if (hasMultipleValues()) {
            return Ints.checkedCast(multiDocValues.getValueCount());
        } else {
            return docValues.getValueCount();
        }
    }

    @Override
    public void close() throws IOException {
    }
}
