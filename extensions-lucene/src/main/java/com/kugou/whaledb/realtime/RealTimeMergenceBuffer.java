package com.kugou.whaledb.realtime;

import com.kugou.whaledb.DirectoryInfo;
import com.kugou.whaledb.DirectoryInfo.DirTpe;
import com.kugou.whaledb.DirectoryInfoWrapper;
import com.kugou.whaledb.codes.WhaledbCodes;
import com.kugou.whaledb.utils.UniqConfig;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with whaledb.
 * User: chiyao
 * Date: 2016/4/7
 * Time: 20:06
 * Description:
 */
public class RealTimeMergenceBuffer {
    private static AtomicLong indexNum = new AtomicLong(0L);
    private ConcurrentHashMap<Long, DirectoryInfo> buffer = new ConcurrentHashMap();

    public RealTimeMergenceBuffer() {
    }

    private static long currIndexNum() {
        return indexNum.getAndDecrement();
    }

    public void mergeBuffer(List<Document> docs) throws IOException {
        long indexNum = currIndexNum();
        DirectoryInfo directoryInfo = new DirectoryInfo(new RAMDirectory());
        IndexWriterConfig iwc = new IndexWriterConfig(new SimpleAnalyzer());
        WhaledbCodes.setCodes(iwc);
        IndexWriter writer = new IndexWriter(directoryInfo.dir, iwc);
        writer.updateDocuments(null, docs);
        writer.close();
        buffer.put(indexNum, directoryInfo);
        mergeBuffer2Buffer();
    }

    public void mergeBuffer2Buffer() throws IOException {
        if (buffer.size() < UniqConfig.INSTANCE().RealTimeMaxIndexCountBuffer()) {
            return;
        }

        DirectoryInfo dirInfo = new DirectoryInfo(new RAMDirectory());
        ArrayList<DirectoryInfoWrapper> list = new ArrayList();
        for (Map.Entry<Long, DirectoryInfo> e : buffer.entrySet()) {
            dirInfo.createtime = Math.min(dirInfo.createtime, e.getValue().createtime);
            list.add(new DirectoryInfoWrapper(e.getKey(), e.getValue()));
        }
        Collections.sort(list);
        int size = UniqConfig.INSTANCE().RealTimeMergeFactorBuffer();
        ArrayList<DirectoryInfo> mergenceList = new ArrayList();
        ArrayList<Long> removeList = new ArrayList();
        for (int i = 0; i < size; i++) {
            mergenceList.add(list.get(i).directoryInfo);
            removeList.add(list.get(i).key);
        }
        dirInfo.tp = DirTpe.ram;
        merge(mergenceList, dirInfo, MergeType.buffer2buffer);
        buffer.remove(removeList);
        long indexNum = currIndexNum();
        buffer.put(indexNum, dirInfo);
    }

    public DirectoryInfo oplimizeBuffer() throws IOException {
        ConcurrentHashMap<Long, DirectoryInfo> current = popBuffer();
        if (current.size() == 0) {
            return null;
        }
        DirectoryInfo dirNew = new DirectoryInfo(new RAMDirectory());
        dirNew.tp = DirTpe.buffer;
        long txid = -1L;
        IndexWriterConfig iwc = new IndexWriterConfig(new SimpleAnalyzer());
        IndexWriter indexWriter = new IndexWriter(dirNew.dir, iwc);
        int i = 0;
        for (Map.Entry<Long, DirectoryInfo> e : current.entrySet()) {
            int j = countDocs(e.getValue().dir);
            if (j > 0) {
                i += j;
                txid = Math.max(txid, e.getValue().readTxid());
                indexWriter.addIndexes(new Directory[]{e.getValue().dir});
            }
        }
//        if (i <= 0) {
//            indexWriter.addDocument(kw.a(paramIndexSchema));
//        }
        indexWriter.forceMerge(1);
        indexWriter.close();
        dirNew.upTxid(txid);
        return dirNew;
    }


    public ConcurrentHashMap<Long, DirectoryInfo> popBuffer() {
        ConcurrentHashMap<Long, DirectoryInfo> current = buffer;
        buffer = new ConcurrentHashMap();
        return current;
    }

    private void merge(ArrayList<DirectoryInfo> directoryInfos, DirectoryInfo to, MergeType mergeType) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(new SimpleAnalyzer());
        WhaledbCodes.setCodes(iwc);
        if (MergeType.finaldisk.equals(mergeType)) {
            if (UniqConfig.INSTANCE().isUseCompoundfileRam2disk()) {
                iwc.setUseCompoundFile(true);
                iwc.getMergePolicy().setNoCFSRatio(1.0D);
            }
        } else if (MergeType.ram2disk.equals(mergeType)) {
            if (UniqConfig.INSTANCE().isUseCompoundfileRam2disk()) {
                iwc.setUseCompoundFile(true);
                iwc.getMergePolicy().setNoCFSRatio(1.0D);
            }
        }
        long txid = -2L;
        int m = 0;
        IndexWriter indexWriter = new IndexWriter(to.dir, iwc);
        for (DirectoryInfo directoryInfo : directoryInfos) {
            if (directoryInfo != null) {
                int n = countDocs(directoryInfo.dir);
                if (n > 0) {
                    m += n;
                    txid = Math.max(txid, directoryInfo.readTxid());
                    indexWriter.addIndexes(new Directory[]{directoryInfo.dir});
                }
            }
        }
//        if (m <= 0) {
//            indexWriter.addDocument(kw.a(paramIndexSchema));
//        }
        indexWriter.forceMerge(1);
        indexWriter.close();
        to.upTxid(txid);
        to.synctxid();
    }

    private static int countDocs(Directory dir) {
        try {
            DirectoryReader reader = DirectoryReader.open(dir);
            return reader.numDocs();
        } catch (Throwable localThrowable) {
        }
        return -1;
    }

}
