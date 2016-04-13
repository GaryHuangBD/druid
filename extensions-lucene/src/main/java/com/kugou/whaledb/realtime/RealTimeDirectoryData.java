package com.kugou.whaledb.realtime;

import com.kugou.whaledb.DirectoryInfo;
import com.kugou.whaledb.DirectoryInfo.DirTpe;
import com.kugou.whaledb.DirectoryInfoWrapper;
import com.kugou.whaledb.utils.UniqConfig;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RealTimeDirectoryData {
    private static AtomicLong indexNum = new AtomicLong(0L);
    private ConcurrentHashMap<String, DirectoryInfo> diskDirector = new ConcurrentHashMap();
    private ConcurrentHashMap<Long, DirectoryInfo> ramDirector = new ConcurrentHashMap();

    private RealTimeDocListBuffer indexBuffer = new RealTimeDocListBuffer();
    private RealTimeMergenceBuffer mergenceBuffer = new RealTimeMergenceBuffer();

    public void addDocument(long uuid, Document doc) {
        indexBuffer.addDocument(uuid, doc);
    }

    public void flush() throws IOException {
        List<Document> flush = indexBuffer.popDoclist();
        mergenceBuffer.mergeBuffer(flush);
    }

    public void mergeRam() throws IOException {
        if (ramDirector.isEmpty()) {
            return;
        }

        while (ramDirector.size() >= 2) {
            ramMerger2Ram(MergeType.ram2diskPre);
        }
    }

    private void ramMerger2Ram(MergeType mergeType) {
        DirectoryInfo dirInfo = new DirectoryInfo(new RAMDirectory());
        ArrayList<DirectoryInfoWrapper> list = new ArrayList();
        for (Map.Entry<Long, DirectoryInfo> e : ramDirector.entrySet()) {
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
//        kz.merge(params.b, localArrayList1, localkh1, mergeType);
        ramDirector.remove(removeList);
        long indexNum = currIndexNum();
        ramDirector.put(Long.valueOf(indexNum), dirInfo);
    }

    private static long currIndexNum() {
        return indexNum.getAndDecrement();
    }
}
