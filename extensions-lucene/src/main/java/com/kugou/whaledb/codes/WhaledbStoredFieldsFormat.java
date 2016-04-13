package com.kugou.whaledb.codes;

import org.apache.log4j.Logger;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;

public class WhaledbStoredFieldsFormat
        extends StoredFieldsFormat {
    private static Logger LOG = Logger.getLogger(WhaledbStoredFieldsFormat.class);

    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos, IOContext ioContext) throws IOException {
        long t1 = System.currentTimeMillis();
        StoredFieldsReader storedFieldsReader = impl().fieldsReader(directory, segmentInfo, fieldInfos, ioContext);
        if (!(directory instanceof RAMDirectory)) {
            long t2 = System.currentTimeMillis() - t1;
            if (storedFieldsReader.ramBytesUsed() > 5120L) {
                LOG.info("StoredFieldsReader:" + segmentInfo.name + ",diff:" + t2 + ",ramused:" + storedFieldsReader.ramBytesUsed() + ",toString:" + storedFieldsReader.toString() + ",directory:" + directory.getClass().getName() + "," + directory.toString());
            }
        }
        return storedFieldsReader;
    }

    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo segmentInfo, IOContext ioContext) throws IOException {
        return impl().fieldsWriter(directory, segmentInfo, ioContext);
    }

    StoredFieldsFormat impl() {
        return new CompressingStoredFieldsFormat("Lucene50StoredFieldsFast", CompressionMode.FAST, 16384, 128, 131072);
    }
}
