package io.druid.segment.column;

import com.metamx.collections.bitmap.MutableBitmap;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by garyhuang on 2016/4/22.
 */
public class LuceneMutableBitmap extends LuceneImmutableBitmap implements MutableBitmap {

    public LuceneMutableBitmap(DocIdSetIterator docIdSetIterator) {
        super(docIdSetIterator);
    }

    public LuceneMutableBitmap() {
        super(DocIdSetIterator.empty());
    }

    @Override
    public void clear() {

    }

    @Override
    public void or(MutableBitmap mutableBitmap) {

    }

    @Override
    public void and(MutableBitmap mutableBitmap) {

    }

    @Override
    public void xor(MutableBitmap mutableBitmap) {

    }

    @Override
    public void andNot(MutableBitmap mutableBitmap) {

    }

    @Override
    public int getSizeInBytes() {
        return 0;
    }

    @Override
    public void add(int entry) {

    }

    @Override
    public void remove(int entry) {

    }

    @Override
    public void serialize(ByteBuffer buffer) {

    }
}
