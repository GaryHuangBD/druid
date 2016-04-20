package io.druid.segment.column;

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;

/**
 *
 */
public class LuceneBitmapIndex implements BitmapIndex{
    private IndexReader indexReader;

    @Override
    public int getCardinality() {
        return 0;
    }

    @Override
    public String getValue(int index) {
        return null;
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
        return null;
    }

    @Override
    public ImmutableBitmap getBitmap(int idx) {
        return null;
    }
}
