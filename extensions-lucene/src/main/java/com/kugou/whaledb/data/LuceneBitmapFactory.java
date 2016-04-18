package com.kugou.whaledb.data;

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
        return null;
    }

    @Override
    public ImmutableBitmap makeEmptyImmutableBitmap() {
        return null;
    }

    @Override
    public ImmutableBitmap makeImmutableBitmap(MutableBitmap mutableBitmap) {
        return null;
    }

    @Override
    public ImmutableBitmap mapImmutableBitmap(ByteBuffer b) {
        return null;
    }

    @Override
    public ImmutableBitmap union(Iterable<ImmutableBitmap> b) {
        return null;
    }

    @Override
    public ImmutableBitmap intersection(Iterable<ImmutableBitmap> b) {
        return null;
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
