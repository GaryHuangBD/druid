package io.druid.segment.column;

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;

import java.nio.ByteBuffer;

/**
 * Created by garyhuang on 2016/4/14.
 */
public class LuceneBitmapFactory implements BitmapFactory {
    @Override
    public MutableBitmap makeEmptyMutableBitmap() {
        return new WrappedLuceneBitmap();
    }

    @Override
    public ImmutableBitmap makeEmptyImmutableBitmap() {
        return makeEmptyMutableBitmap();
    }

    @Override
    public ImmutableBitmap makeImmutableBitmap(MutableBitmap mutableBitmap) {
        return mutableBitmap;
    }

    @Override
    public ImmutableBitmap mapImmutableBitmap(ByteBuffer b) {
        try {
            return new WrappedLuceneBitmap(b);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public ImmutableBitmap union(Iterable<ImmutableBitmap> b) {
        WrappedLuceneBitmap luceneBitmap = null;
        for (ImmutableBitmap bitmap : b) {
            if (luceneBitmap == null) {
                luceneBitmap = new WrappedLuceneBitmap(((WrappedLuceneBitmap)bitmap).docIdSetIterator);
            } else {
                luceneBitmap.union(bitmap);
            }

        }
        return luceneBitmap;
    }

    @Override
    public ImmutableBitmap intersection(Iterable<ImmutableBitmap> b) {
        WrappedLuceneBitmap luceneBitmap = null;
        for (ImmutableBitmap bitmap : b) {
            if (luceneBitmap == null) {
                luceneBitmap = new WrappedLuceneBitmap(((WrappedLuceneBitmap)bitmap).docIdSetIterator);
            } else {
                luceneBitmap.intersection(bitmap);
            }

        }
        return luceneBitmap;
    }

    @Override
    public ImmutableBitmap complement(ImmutableBitmap b) {
        return null;
    }

    @Override
    public ImmutableBitmap complement(ImmutableBitmap b, int length) {
        return null;
    }
}
