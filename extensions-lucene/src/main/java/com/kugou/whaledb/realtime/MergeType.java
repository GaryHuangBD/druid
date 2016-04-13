package com.kugou.whaledb.realtime;

/**
 * Created with whaledb.
 * User: chiyao
 * Date: 2016/4/12
 * Time: 11:40
 * Description:
 */
public enum MergeType {
    finaldisk, ram2diskPre, ram2disk, ram2ram, buffer2buffer, buffer2ram, doclist2buffer;

    private MergeType() {
    }
}