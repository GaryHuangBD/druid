package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.metamx.common.guava.FunctionalIterable;
import io.druid.segment.Rowboat;
import io.druid.segment.column.ValueType;
import org.apache.lucene.document.*;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class LuceneDocumentTransformer {
    private final IncrementalIndex index;

    public LuceneDocumentTransformer(IncrementalIndex index){
        this.index = index;
    }

    public Iterable<Document> transform() {
        Iterable<Document> docs = FunctionalIterable
            .create(index.getFacts().entrySet())
            .transform( new Function<Map.Entry<IncrementalIndex.TimeAndDims, Integer>, Document>() {
                @Override
                public Document apply(@Nullable Map.Entry<IncrementalIndex.TimeAndDims, Integer> input) {
                    List<String> dimensions = index.getDimensions();
                    final IncrementalIndex.TimeAndDims timeAndDims = input.getKey();
                    final String[][] dimValues = timeAndDims.getDims();
                    Document doc = new Document();
                    String fName;
                    for(int i=0; i<dimensions.size(); i++){
                        fName = dimensions.get(i);
                        for(String fVal : dimValues[i]){
                            doc.add(createField(fName, index.getCapabilities(fName).getType(), fVal));
                        }
                    }
                    return doc;
                }
            });
        return docs;
    }

    private Field createField(String name, ValueType type, String val){
        switch (type){
            case STRING:
                return new StringField(name, val, Field.Store.NO);
            case FLOAT:
                return new FloatField(name, Float.parseFloat(val), Field.Store.NO);
            case LONG:
                return new LongField(name, Long.parseLong(val), Field.Store.NO);
            case COMPLEX:
            default:
                throw new UnsupportedOperationException();
        }
    }
}
