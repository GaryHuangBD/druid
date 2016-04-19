package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.metamx.common.guava.FunctionalIterable;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.util.ArrayList;
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
                    // add time field
                    doc.add(new LongField(Column.TIME_COLUMN_NAME, timeAndDims.getTimestamp(), Field.Store.NO));
                    String fName;
                    for(int i=0; i<dimensions.size(); i++){
                        fName = dimensions.get(i);
                        if (null == dimValues[i]) {
                            continue;
                        }
                        for(String fVal : dimValues[i]){
                            if (null == fVal) {
                                continue;
                            }
                            for (IndexableField field: createFields(fName, index.getCapabilities(fName), fVal)) {
                                doc.add(field);
                            }
                        }
                    }
                    return doc;
                }
            });
        return docs;
    }

    private List<IndexableField> createFields(String name, ColumnCapabilities capabilities, String val){
        List<IndexableField> fields = new ArrayList<>();
        boolean hasMultipleValues = capabilities.hasMultipleValues();
        switch (capabilities.getType()){
            case STRING:
                fields.add(new StringField(name, val, Field.Store.NO));
                final BytesRef bytes = new BytesRef(val);
                if (hasMultipleValues) {
                    fields.add(new SortedSetDocValuesField(name, bytes));
                } else {
                    fields.add(new SortedDocValuesField(name, bytes));
                }
                break;
            case FLOAT:
                fields.add(new FloatField(name, Float.parseFloat(val), Field.Store.NO));
                break;
            case LONG:
                fields.add(new LongField(name, Long.parseLong(val), Field.Store.NO));
                break;
            case COMPLEX:
            default:
                break;
        }
        return fields;
    }
}
