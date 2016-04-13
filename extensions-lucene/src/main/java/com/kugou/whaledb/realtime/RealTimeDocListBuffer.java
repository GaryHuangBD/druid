package com.kugou.whaledb.realtime;

import com.kugou.whaledb.utils.UniqConfig;
import org.apache.lucene.document.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with whaledb.
 * User: chiyao
 * Date: 2016/4/7
 * Time: 20:35
 * Description:
 */
public class RealTimeDocListBuffer {
   private List<Document> doclistBuffer = new ArrayList(UniqConfig.INSTANCE().RealTimeDoclistBuffer());
    private Object lock = new Object();

    public void addDocument(long uuid, Document doc) {
        synchronized (lock) {
            doclistBuffer.add(doc);
        }
    }

    public List<Document> popDoclist(){
        int size = doclistBuffer.size();
        if (size <= 0) {
            return null;
        }

        List<Document> flush = doclistBuffer;
        doclistBuffer = new ArrayList<Document>(UniqConfig.INSTANCE().RealTimeDoclistBuffer());
        return flush;
    }

}
