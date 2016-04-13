package com.kugou.whaledb.utils;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by garyhuang on 2016/4/12.
 */
public class ConvertUtilTest {
    @Test
    public void parseInt() throws Exception {
        Assert.assertEquals(0, ConvertUtil.parseInt("null"));
        Assert.assertEquals(0, ConvertUtil.parseInt(""));
        Assert.assertEquals(0, ConvertUtil.parseInt(null));
        Assert.assertEquals(1234, ConvertUtil.parseInt("1234"));
        Assert.assertEquals(0, ConvertUtil.parseInt("123abc"));
    }

}