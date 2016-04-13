package com.kugou.whaledb.realtime;

import org.apache.lucene.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RealTimeDirectory {
    private RealTimeDirectoryData data;

    public RealTimeDirectory(String baseDir) {
        data = new RealTimeDirectoryData();
    }

    public void addDocument(Document doc, boolean writelog) {
        long uuid = 0l;
        data.addDocument(uuid, doc);
    }

    public void addDocument(Document doc) {
        addDocument(doc, true);
    }

    public void flush() throws IOException {
        data.flush();
    }
}
