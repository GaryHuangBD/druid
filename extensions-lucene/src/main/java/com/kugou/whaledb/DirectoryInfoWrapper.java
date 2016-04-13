package com.kugou.whaledb;

/**
 * Created with whaledb.
 * User: chiyao
 * Date: 2016/4/12
 * Time: 12:12
 * Description:
 */
public class DirectoryInfoWrapper implements Comparable<DirectoryInfoWrapper> {
    public final long key;
    public final DirectoryInfo directoryInfo;

    public DirectoryInfoWrapper(long key, DirectoryInfo directoryInfo) {
        this.key = key;
        this.directoryInfo = directoryInfo;
    }

    @Override
    public int compareTo(DirectoryInfoWrapper o) {
        return directoryInfo.filelength().compareTo(o.directoryInfo.filelength());
    }
}
