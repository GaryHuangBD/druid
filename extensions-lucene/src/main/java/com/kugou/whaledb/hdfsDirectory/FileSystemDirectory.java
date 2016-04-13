package com.kugou.whaledb.hdfsDirectory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FileSystemDirectory extends BaseDirectory {

    protected final Path path;
    protected final Configuration conf;
    private final FileSystem fs;
    private int bufferSize = 8192;

    public FileSystemDirectory(Path path, LockFactory lockFactory, Configuration conf, FileSystem fs) {
        super(lockFactory);
        this.path = path;
        this.conf = new Configuration(conf);
        this.fs = fs;
        this.fs.setConf(this.conf);
    }

    public String[] listAll() throws IOException {
        FileStatus[] fileStatuses = listStatus(path);
        ArrayList<String> rtn = new ArrayList();
        if (fileStatuses == null) {
            return new String[0];
        }
        for (FileStatus fileStatus : fileStatuses) {
            if (!fileStatus.isDirectory()) {
                rtn.add(fileStatus.getPath().getName());
            }
        }
        return toArray(rtn);
    }

    private FileStatus[] listStatus(Path path) throws IOException {
        if (fs.exists(path)) {
            return fs.listStatus(path);
        }
        return null;
    }

    private String[] toArray(List<String> list) {
        int k = list.size();
        for (int i = 0; i < k; i++) {
            list.set(i, cutSuffix(list.get(i)));
        }
        return list.toArray(new String[0]);
    }

    private String cutSuffix(String name) {
        if (name.endsWith(".lf")) {
            return name.substring(0, name.length() - 3);
        }
        return name;
    }

    public void deleteFile(String name) throws IOException {
        Path localPath = new Path(path, name);
        fs.delete(localPath, false);
    }

    private FileStatus fileStatus(Path path) throws IOException {
        if (fs.exists(path)) {
            return fs.getFileStatus(path);
        }
        return null;
    }

    public long fileLength(String name) throws IOException {
        Path localPath = new Path(path, name);
        FileStatus fileStatus = fileStatus(localPath);
        return fileStatus.getLen();
    }

    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return new FileSystemIndexOutput(fs, new Path(path, name), bufferSize);
    }

    public void sync(Collection<String> names) throws IOException {
    }

    public void renameFile(String source, String dest) throws IOException {
        fs.rename(new Path(path, source), new Path(path, dest));
    }

    public IndexInput openInput(String name, IOContext context) throws IOException {
        return new FileSystemIndexInput(fs, new Path(path, name), bufferSize);
    }

    public void close() throws IOException {
    }
}
