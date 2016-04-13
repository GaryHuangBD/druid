package com.kugou.whaledb;

import com.kugou.whaledb.realtime.RealTimeDirectory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Administrator on 2016/4/7 0007.
 */
public class RealTimeTest {
    @Test
    public void testRealTimeDirectory() throws IOException {
        RealTimeDirectory directory = new RealTimeDirectory("/data1");
        for (int i = 0 ;i<10;i++){
            Document document = new Document();
            document.add(new StringField("name", "yaotc"+i, Field.Store.YES));
            directory.addDocument(document);
        }
        directory.flush();
    }
}
