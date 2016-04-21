package io.druid.segment.column;

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 *
 */
public class LuceneBitmapIndex implements BitmapIndex{
    private final DictionaryEncodedColumn dictionaryEncodedColumn;
    private final TermsEnum termsEnum;

    public LuceneBitmapIndex(TermsEnum termsEnum, DictionaryEncodedColumn dictionaryEncodedColumn) {
        this.dictionaryEncodedColumn = dictionaryEncodedColumn;
        this.termsEnum = termsEnum;
    }

    @Override
    public int getCardinality() {
        return dictionaryEncodedColumn.getCardinality();
    }

    @Override
    public String getValue(int index) {
        return dictionaryEncodedColumn.lookupName(index);
    }

    @Override
    public boolean hasNulls() {
        return false;
    }

    @Override
    public BitmapFactory getBitmapFactory() {
        return null;
    }

    @Override
    public ImmutableBitmap getBitmap(String value) {
        try {
            if(termsEnum.seekExact(new BytesRef(value))) {
                return new LuceneImmutableBitmap(termsEnum.postings(null));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public ImmutableBitmap getBitmap(int idx) {
        return getBitmap(getValue(idx));
    }
}
