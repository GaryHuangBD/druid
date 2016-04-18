package io.druid.segment.codes;

import org.apache.lucene.codecs.compressing.CompressingTermVectorsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;

/*    */
public class WhaledbTermVectorsFormat extends CompressingTermVectorsFormat {
    public WhaledbTermVectorsFormat() {
        super("WhaledbTermVectors", "", CompressionMode.FAST, 4096, 131072);
    }
}
