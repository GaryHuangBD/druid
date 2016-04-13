package com.kugou.whaledb.utils;


/**
 * Created by garyhuang on 2016/4/12.
 */
public class ConvertUtil {

    public static int parseInt(String paramString) {
        if ((paramString == null) || (paramString.isEmpty()) || (paramString.equals("null"))) {
            return 0;
        }
        try {
            return Integer.parseInt(paramString);
        } catch (Throwable localThrowable) {}
        return 0;
    }

    public static float parseFloat(String paramString) {
        if ((paramString == null) || (paramString.isEmpty()) || (paramString.equals("null"))) {
            return 0.0F;
        }
        try {
            return Float.parseFloat(paramString);
        } catch (Throwable localThrowable) {}
        return 0.0F;
    }
    public static long parseLong(String paramString) {
        if ((paramString == null) || (paramString.isEmpty()) || (paramString.equals("null"))) {
            return 0L;
        }
        try {
            return Long.parseLong(paramString);
        } catch (Throwable localThrowable) {}
        return 0L;
    }
    public static final double parseDouble(String paramString)
    {
        if ((paramString == null) || (paramString.isEmpty()) || (paramString.equals("null"))) {
            return 0.0D;
        }
        try {
            return Double.parseDouble(paramString);
        } catch (Throwable localThrowable) {}
        return 0.0D;
    }

}
