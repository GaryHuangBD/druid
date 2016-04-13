package com.kugou.whaledb.hdfsDirectory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FileSystemIndexInput extends BufferedIndexInput {
    private boolean isClone = false;
    private Descriptor descriptor;

    public FileSystemIndexInput(FileSystem fs, Path path, int bufferSize) throws IOException {
        super("fileSystem=" + fs + " path=" + path, bufferSize);
        descriptor = new Descriptor(fs, path, bufferSize);
    }

    protected void finalize() throws IOException {
        if (!isClone) {
            descriptor.close();
        }
    }

    protected void seekInternal(long pos) throws IOException {
    }

    protected void readInternal(byte[] b, int offset, int len) throws IOException {
        readFully(getFilePointer(), b, offset, len);
    }

    private void readFully(long position, byte[] b, int offset, int len) throws IOException {
        descriptor.readFully(position, b, offset, len);
    }


    public long length() {
        return descriptor.length();
    }
}
