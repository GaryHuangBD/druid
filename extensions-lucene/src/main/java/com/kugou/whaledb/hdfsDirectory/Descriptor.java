package com.kugou.whaledb.hdfsDirectory;

import com.kugou.whaledb.adhoc.TimeCacheMapLru;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Descriptor {
    private static final Log logger = LogFactory.getLog(Descriptor.class);
    private static TimeCacheMapLru.ExpiredCallback<String, Descriptor> callback = new TimeCacheMapLru.ExpiredCallback<String, Descriptor>() {
        public void expire(String key, Descriptor val) {
            synchronized (val) {
                try {
                    val.close();
                } catch (IOException e) {
                    logger.error("expire", e);
                }
            }
        }

        public void commit() {

        }

    };
    private static TimeCacheMapLru<String, Descriptor> RamDirector = new TimeCacheMapLru<String, Descriptor>(1024, 600, 3, callback);

    private String uuid = java.util.UUID.randomUUID().toString();
    private FSDataInputStream in;
    private long position; // cache of in.getPos()
    private Path file;

    private FileSystem fs;
    private int ioFileBufferSize;
    long index = 0;

    private AtomicLong len = new AtomicLong(-1L);

    public Descriptor(FileSystem fs, Path _file, int ioFileBufferSize)
            throws IOException {
        this.position = 0l;
        this.file = _file;
        this.fs = fs;
        this.ioFileBufferSize = ioFileBufferSize;
    }

    public FSDataInputStream stream() throws IOException {
        if (in == null) {
            in = fs.open(file, ioFileBufferSize);
            if (position > 0) {
                in.seek(position);
            }
            RamDirector.put(uuid, this);
        }

        if (index++ > 1024L) {
            RamDirector.put(uuid, this);
            index = 0;
        }

        return in;
    }

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public void readFully(long position, byte[] b, int offset, int len) throws IOException {
        lock.readLock().lock();
        try {
            stream().readFully(position, b, offset, len);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void close() throws IOException {
        try {
            lock.writeLock().lock();
            if (in != null) {
                in.close();
            }
        } finally {
            in = null;
            lock.writeLock().unlock();
        }
    }

    public long length() {
        if (len.get() >= 0L) {
            return len.get();
        }
        try {
            FileStatus localFileStatus = fs.getFileStatus(file);
            long l1 = localFileStatus.getLen();
            len.set(l1);
            return l1;
        } catch (IOException localIOException) {
            throw new RuntimeException(localIOException);
        }
    }

}