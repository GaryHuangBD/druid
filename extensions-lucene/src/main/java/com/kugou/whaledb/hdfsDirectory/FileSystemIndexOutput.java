package com.kugou.whaledb.hdfsDirectory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.IndexOutput;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

public class FileSystemIndexOutput extends IndexOutput {

    private final CheckedOutputStream out;
    private final CRC32 crc32 = new CRC32();
    private long pos = 0L;

    public FileSystemIndexOutput(FileSystem fs, Path path, int ioFileBufferSize)
            throws IOException {
        super("fileSystem=" + fs + " path=" + path);
        out = new CheckedOutputStream(new BufferedOutputStream(fs.create(path, true, ioFileBufferSize)), crc32);
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public long getFilePointer() {
        return pos;
    }

    @Override
    public long getChecksum() throws IOException {
        return crc32.getValue();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        out.write(b);
        pos += 1L;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        out.write(b, offset, length);
        pos += length;
    }
}
