package io.druid.segment.column;

import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.logger.Logger;
import org.apache.lucene.search.DocIdSetIterator;
import org.roaringbitmap.IntIterator;

import java.io.*;
import java.nio.ByteBuffer;

/**
 *
 */
public class LuceneImmutableBitmap implements ImmutableBitmap {
    private static final Logger log = new Logger(LuceneImmutableBitmap.class);

    protected DocIdSetIterator docIdSetIterator;

    protected LuceneImmutableBitmap(ByteBuffer byteBuffer) throws Exception {
        ByteArrayInputStream bi = new ByteArrayInputStream(byteBuffer.array());
        ObjectInputStream oi = new ObjectInputStream(bi);
        this.docIdSetIterator = (DocIdSetIterator)oi.readObject();
    }

    public LuceneImmutableBitmap(DocIdSetIterator docIdSetIterator1){
        this.docIdSetIterator = docIdSetIterator1;

    }

    public LuceneImmutableBitmap(){
        this(DocIdSetIterator.empty());
    }


    private class DocIdsIterator implements IntIterator {
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
            DocIdsIterator newIt = new DocIdsIterator();
            return newIt;
        }
    }

    @Override
    public IntIterator iterator() {
        return new DocIdsIterator();
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
        DocIdSetIterator otherDocIdsIterator = ((LuceneImmutableBitmap)other).docIdSetIterator;
        return this.docIdSetIterator.docID() - otherDocIdsIterator.docID();
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
        WrappedLuceneBitmap retval = new WrappedLuceneBitmap(docIdSetIterator);
        retval.or((WrappedLuceneBitmap)otherBitmap);
        return retval;
    }

    @Override
    public ImmutableBitmap intersection(ImmutableBitmap otherBitmap) {
        WrappedLuceneBitmap retval = new WrappedLuceneBitmap(docIdSetIterator);
        retval.and((WrappedLuceneBitmap)otherBitmap);
        return retval;
    }

    @Override
    public ImmutableBitmap difference(ImmutableBitmap otherBitmap) {
        WrappedLuceneBitmap retval = new WrappedLuceneBitmap(docIdSetIterator);
        retval.andNot((WrappedLuceneBitmap)otherBitmap);
        return retval;
    }

}
