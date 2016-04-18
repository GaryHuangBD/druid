package io.druid.segment.codes;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene53.Lucene53Codec;
import org.apache.lucene.index.IndexWriterConfig;

/**
 * 
 */
public class WhaledbCodec
        extends FilterCodec {
    private final TermVectorsFormat vectorsFormat = new WhaledbTermVectorsFormat();
    private final StoredFieldsFormat storedFieldsFormat = new WhaledbStoredFieldsFormat();

    public WhaledbCodec() {
        super("WhaledbCodec", new Lucene53Codec());
    }

    public final StoredFieldsFormat storedFieldsFormat() {
        return this.storedFieldsFormat;
    }

    public final TermVectorsFormat termVectorsFormat() {
        return this.vectorsFormat;
    }

    public static void setCodes(IndexWriterConfig iwc) {
        iwc.setCodec(new WhaledbCodec());
    }
}