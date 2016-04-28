package io.druid.segment.data;

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.logger.Logger;
import io.druid.segment.column.LuceneBitmapFactory;
import io.druid.segment.column.WrappedLuceneBitmap;

import java.nio.ByteBuffer;

/**
 * Created by garyhuang on 2016/4/26.
 */
public class LuceneBitmapSerdeFactory implements BitmapSerdeFactory {

    private static final Logger log = new Logger(LuceneBitmapSerdeFactory.class);

    private static final ObjectStrategy<ImmutableBitmap> objectStrategy = new LuceneBitmapSerdeFactory.ImmutableLuceneBitmapObjectStrategy();
    private static final BitmapFactory bitmapFactory = new LuceneBitmapFactory();

    @Override
    public ObjectStrategy<ImmutableBitmap> getObjectStrategy() {
        return objectStrategy;
    }

    @Override
    public BitmapFactory getBitmapFactory() {
        return bitmapFactory;
    }

    private static class ImmutableLuceneBitmapObjectStrategy
            implements ObjectStrategy<ImmutableBitmap>
    {
        @Override
        public Class<ImmutableBitmap> getClazz()
        {
            return ImmutableBitmap.class;
        }

        @Override
        public ImmutableBitmap fromByteBuffer(ByteBuffer buffer, int numBytes) {
            final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
            readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
            try {
                return new WrappedLuceneBitmap(readOnlyBuffer);
            } catch (Exception e) {
                log.error("cannot convert from ByteBuffer to ImmutableBitmap", e);
                return null;
            }

        }

        @Override
        public byte[] toBytes(ImmutableBitmap val) {
            if (val == null || val.size() == 0) {
                return new byte[]{};
            }
            return val.toBytes();
        }

        @Override
        public int compare(ImmutableBitmap o1, ImmutableBitmap o2) {
            return o1.compareTo(o2);
        }
    }

    @Override
    public String toString()
    {
        return "RoaringBitmapSerdeFactory{}";
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || o instanceof RoaringBitmapSerdeFactory;
    }

    @Override
    public int hashCode()
    {
        return 0;
    }
}
