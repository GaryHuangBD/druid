package com.kugou.whaledb.codes;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene53.Lucene53Codec;
import org.apache.lucene.index.IndexWriterConfig;

/**
 * Created with whaledb.
 * User: chiyao
 * Date: 2016/4/8
 * Time: 11:23
 * Description:
 */
public class WhaledbCodes
        extends FilterCodec {
    private final TermVectorsFormat vectorsFormat = new WhaledbTermVectorsFormat();
    private final StoredFieldsFormat storedFieldsFormat = new WhaledbStoredFieldsFormat();

    public WhaledbCodes() {
        super("WhaledbCodes", new Lucene53Codec());
    }

    public final StoredFieldsFormat storedFieldsFormat() {
        return this.storedFieldsFormat;
    }

    public final TermVectorsFormat termVectorsFormat() {
        return this.vectorsFormat;
    }

    public static void setCodes(IndexWriterConfig iwc) {
        iwc.setCodec(new WhaledbCodes());
    }
}