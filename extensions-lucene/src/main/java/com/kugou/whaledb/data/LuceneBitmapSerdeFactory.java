package com.kugou.whaledb.data;

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ObjectStrategy;

/**
 * Created by garyhuang on 2016/4/14.
 */
public class LuceneBitmapSerdeFactory implements BitmapSerdeFactory {

    private static final BitmapFactory bitmapFactory = new LuceneBitmapFactory();

    @Override
    public ObjectStrategy<ImmutableBitmap> getObjectStrategy() {
        return null;
    }

    @Override
    public BitmapFactory getBitmapFactory() {
        return bitmapFactory;
    }
}
