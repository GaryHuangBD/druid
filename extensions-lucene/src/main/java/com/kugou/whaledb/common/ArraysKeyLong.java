package com.kugou.whaledb.common;

import java.util.Arrays;

/**
 * Created by garyhuang on 2016/4/13.
 */
public class ArraysKeyLong implements java.io.Serializable {

    private static final long serializeUUid = 1L;

    private long[] k;


    public ArraysKeyLong(long[] paramArrayOfLong) {
        this.k = paramArrayOfLong;
    }

    public int hashCode() {
        return 31 + Arrays.hashCode(this.k);
    }




    public boolean equals(Object paramObject) {
        if (this == paramObject)
            return true;
        if (paramObject == null)
            return false;
        if (getClass() != paramObject.getClass())
            return false;
        ArraysKeyLong other = (ArraysKeyLong)paramObject;
        if (!Arrays.equals(this.k, other.k))
            return false;
        return true;
    }


    public String toString() {
        return "ArraysKeyLong [k=" + Arrays.toString(this.k) + "]";
    }

}
