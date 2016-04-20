package io.druid.segment.column;

import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.ImmutableBitmap;
import javafx.geometry.Pos;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/4/19 0019.
 */
public class LuceneImmutableBitmap implements ImmutableBitmap {
    private DocIdSetIterator docIdSetIterator;

    public LuceneImmutableBitmap(DocIdSetIterator docIdSetIterator){
        this.docIdSetIterator = docIdSetIterator;
    }

    @Override
    public IntIterator iterator() {
        return new IntIterator() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public int next() {
                return 0;
            }

            @Override
            public IntIterator clone() {
                return null;
            }
        };
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public byte[] toBytes() {
        return new byte[0];
    }

    @Override
    public int compareTo(ImmutableBitmap other) {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return docIdSetIterator.cost() == 0;
    }

    @Override
    public boolean get(int value) {
        int currDoc = docIdSetIterator.docID();
        try {
            docIdSetIterator.advance(value);
            int resDoc = docIdSetIterator.docID();
            return value == resDoc;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    @Override
    public ImmutableBitmap union(ImmutableBitmap otherBitmap) {

        return null;
    }

    @Override
    public ImmutableBitmap intersection(ImmutableBitmap otherBitmap) {
        List<DocIdSetIterator> list = Lists.newArrayList(docIdSetIterator);
        return new LuceneImmutableBitmap(ConjunctionDISI.intersect(list));
    }

    @Override
    public ImmutableBitmap difference(ImmutableBitmap otherBitmap) {
        return null;
    }

}
