package io.druid.segment.column;

import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.MutableBitmap;
import org.apache.lucene.search.*;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by garyhuang on 2016/4/22.
 */
public class WrappedLuceneBitmap extends LuceneImmutableBitmap implements MutableBitmap {

    public WrappedLuceneBitmap(DocIdSetIterator docIdSetIterator) {
        super(docIdSetIterator);
    }

    public WrappedLuceneBitmap() {
        super();
    }

    public WrappedLuceneBitmap(ByteBuffer byteBuffer) throws Exception {
        super(byteBuffer);
    }

    @Override
    public void clear() {

    }

    @Override
    public void or(MutableBitmap mutableBitmap) {
        if (mutableBitmap instanceof WrappedLuceneBitmap) {
            WrappedLuceneBitmap bitmap = (WrappedLuceneBitmap)mutableBitmap;
            List<DocIdSetIterator> list = Lists.newArrayList(docIdSetIterator);
            list.add(bitmap.docIdSetIterator);

            DisiPriorityQueue<DocIdSetIterator> subDocIdSetIterator = new DisiPriorityQueue<>(list.size());
            for (DocIdSetIterator docIds : list) {
                subDocIdSetIterator.add(new DisiWrapper<>(docIds));
            }
            this.docIdSetIterator =  new DisjunctionDISIApproximation(subDocIdSetIterator);
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Unknown class type: %s  expected %s",
                            mutableBitmap.getClass().getCanonicalName(),
                            WrappedLuceneBitmap.class.getCanonicalName()
                    )
            );
        }
    }

    @Override
    public void and(MutableBitmap mutableBitmap) {
        if (mutableBitmap instanceof WrappedLuceneBitmap) {
            WrappedLuceneBitmap bitmap = (WrappedLuceneBitmap)mutableBitmap;
            List<DocIdSetIterator> list = Lists.newArrayList(docIdSetIterator);
            list.add(bitmap.docIdSetIterator);
            this.docIdSetIterator =  ConjunctionDISI.intersect(list);
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Unknown class type: %s  expected %s",
                            mutableBitmap.getClass().getCanonicalName(),
                            WrappedLuceneBitmap.class.getCanonicalName()
                    )
            );
        }
    }

    @Override
    public void xor(MutableBitmap mutableBitmap) {
        throw new UnsupportedOperationException(
                "does not support xor"
        );
    }

    @Override
    public void andNot(MutableBitmap mutableBitmap) {
        throw new UnsupportedOperationException(
                "does not support andNot"
        );
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
