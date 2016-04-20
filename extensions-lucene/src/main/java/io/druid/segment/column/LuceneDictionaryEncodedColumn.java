package io.druid.segment.column;

import com.amazonaws.services.devicefarm.model.Run;
import com.google.common.primitives.Ints;
import io.druid.segment.data.IndexedInts;
import org.apache.lucene.index.OrdTermState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 *
 */
public class LuceneDictionaryEncodedColumn implements DictionaryEncodedColumn {
    private final TermsEnum termsEnum;
    private final int length;
    private final int cardinality;

    public LuceneDictionaryEncodedColumn(Terms terms) throws IOException {
        termsEnum = terms.iterator();
        length = terms.getDocCount();
        cardinality = Ints.checkedCast(terms.size());
    }

    @Override
    public int length() {
        return length();
    }

    @Override
    public boolean hasMultipleValues() {
        return false;
    }

    @Override
    public int getSingleValueRow(int rowNum) {
        return 0;
    }

    @Override
    public IndexedInts getMultiValueRow(int rowNum) {
        return null;
    }

    @Override
    public String lookupName(int id) {
        try {
            termsEnum.seekExact(id);
            return termsEnum.term().utf8ToString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int lookupId(String name) {
        try {
            if (termsEnum.seekExact(new BytesRef(name))) {
                OrdTermState state = (OrdTermState)termsEnum.termState();
                return Ints.checkedCast(state.ord);
            }
            return -1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getCardinality() {
        return cardinality;
    }

    @Override
    public void close() throws IOException {
    }
}
