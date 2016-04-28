package io.druid.segment.data;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;

/**
 * Created by garyhuang on 2016/4/26.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = BitmapSerde.DefaultBitmapSerdeFactory.class)
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = "concise", value = ConciseBitmapSerdeFactory.class),
        @JsonSubTypes.Type(name = "roaring", value = RoaringBitmapSerdeFactory.class),
        @JsonSubTypes.Type(name = "lucene", value = LuceneBitmapSerdeFactory.class)
})
public interface BitmapSerdeFactory {
    public ObjectStrategy<ImmutableBitmap> getObjectStrategy();

    public BitmapFactory getBitmapFactory();
}
