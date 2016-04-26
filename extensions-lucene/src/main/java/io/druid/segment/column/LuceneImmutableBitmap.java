package io.druid.segment.column;

import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.logger.Logger;
import org.apache.lucene.search.*;
import org.roaringbitmap.IntIterator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;

/**
 *
 */
public class LuceneImmutableBitmap implements ImmutableBitmap {
    private static final Logger log = new Logger(LuceneImmutableBitmap.class);

    private final DocIdSetIterator docIdSetIterator;

    public LuceneImmutableBitmap(DocIdSetIterator docIdSetIterator1){
        this.docIdSetIterator = docIdSetIterator1;


    }

    public LuceneImmutableBitmap(){
        this(DocIdSetIterator.empty());
    }

    @Override
    public IntIterator iterator() {
        return new IntIterator() {
            @Override
            public boolean hasNext() {
                return docIdSetIterator.docID() == DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int next() {
                try {
                    return docIdSetIterator.nextDoc();
                } catch (IOException e) {
                    log.error("cannot find docIdSetIterator nextDoc", e);
                }
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public IntIterator clone() {
                return null;
            }
        };
    }

    @Override
    public int size() {
        return (int)docIdSetIterator.cost();
    }

    @Override
    public byte[] toBytes() {
        byte[] bytes = null;
        try {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            ObjectOutputStream oo = new ObjectOutputStream(bo);
            oo.writeObject(docIdSetIterator);

            bytes = bo.toByteArray();

            bo.close();
            oo.close();
        } catch (Exception e) {
            log.error("cannot translation docIdSetIterator to byte[]", e);
        }
        return bytes;
    }

    @Override
    public int compareTo(ImmutableBitmap other) {
        return this.docIdSetIterator.docID() - other.iterator().next();
    }

    @Override
    public boolean isEmpty() {
        return docIdSetIterator.cost() == 0;
    }

    @Override
    public boolean get(int value) {
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
        List<DocIdSetIterator> list = Lists.newArrayList(docIdSetIterator);
        if (otherBitmap instanceof DocIdSetIterator) {
            list.add((DocIdSetIterator)otherBitmap);
        }
        DisiPriorityQueue<DocIdSetIterator> subDocIdSetIterator = new DisiPriorityQueue<>(list.size());
        for (DocIdSetIterator docIds : list) {
            subDocIdSetIterator.add(new DisiWrapper<>(docIds));
        }
        DocIdSetIterator it =  new DisjunctionDISIApproximation(subDocIdSetIterator);
        return new LuceneImmutableBitmap(it);
    }

    @Override
    public ImmutableBitmap intersection(ImmutableBitmap otherBitmap) {
        List<DocIdSetIterator> list = Lists.newArrayList(docIdSetIterator);
        if (otherBitmap instanceof DocIdSetIterator) {
            list.add((DocIdSetIterator)otherBitmap);
        }
        return new LuceneImmutableBitmap(ConjunctionDISI.intersect(list));
    }

    @Override
    public ImmutableBitmap difference(ImmutableBitmap otherBitmap) {
        return null;
    }

}
