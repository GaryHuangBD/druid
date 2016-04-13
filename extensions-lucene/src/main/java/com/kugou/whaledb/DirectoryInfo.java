package com.kugou.whaledb;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DirectoryInfo {
    public static Logger LOG = LoggerFactory.getLogger(DirectoryInfo.class);

    public static enum DirTpe {
        hdfs, file, ram, buffer, DirTpe, delete
    }

    public final Directory dir;
    public long createtime = System.currentTimeMillis();
    public DirTpe tp = DirTpe.file;
    public long txid = 0;

    public DirectoryInfo(Directory dir) {
        this.dir = dir;
    }

    public void upTxid(long txidp) {
        this.txid = Math.max(this.txid, txidp);
    }

    public long readTxid() throws IOException {
        if (txid > 0) {
            return txid;
        }
        if (isExisted()) {
            return 0l;
        }

        IndexInput in = dir.openInput("txid", IOContext.DEFAULT);
        long rtn = 0l;
        try {
            rtn = Long.parseLong(in.readString());
        } catch (Throwable e) {
            rtn = 0;
        }
        in.close();
        return rtn;
    }

    public void synctxid() throws IOException {
        if (isExisted()) {
            dir.deleteFile("txid");
        }
        IndexOutput out = dir.createOutput("txid", IOContext.DEFAULT);
        out.writeString(String.valueOf(txid));
        out.close();
        if (txid > 0L) {
            IndexOutput localIndexOutput = dir.createOutput("txid", IOContext.DEFAULT);
            localIndexOutput.writeString(String.valueOf(txid));
            localIndexOutput.close();
        }
    }

    public Long filelength() {
        Long rtn = 0l;
        try {
            String[] list = dir.listAll();
            if (list != null) {
                for (String d : list) {
                    try {
                        rtn += dir.fileLength(d);
                    } catch (Throwable e) {
                        LOG.error("getfilelen:" + dir.toString(), e);

                    }
                }
            }
        } catch (Throwable e) {
            LOG.error("filelength", e);
        }
        return rtn;
    }

    public boolean isExisted() {
        try {
            String[] files = dir.listAll();
            if (files != null) {
                for (String f : files) {
                    if (f.equals("txid")) {
                        return true;
                    }
                }
            }
        } catch (Throwable localThrowable) {
        }
        return false;
    }
}
