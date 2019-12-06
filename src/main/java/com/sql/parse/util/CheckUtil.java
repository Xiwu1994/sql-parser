package com.sql.parse.util;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

public final class CheckUtil {


    public static boolean isEmpty(final boolean[] value) {
        return null == value || value.length == 0;
    }

    public static boolean isEmpty(final byte[] a) {
        return null == a || a.length == 0;
    }


    public static boolean isEmpty(final char[] value) {
        return null == value || value.length == 0;
    }

    public static boolean isEmpty(final CharSequence s) {
        return null == s || s.length() == 0;
    }

    public static boolean isEmpty(final CharSequence[] value) {
        return null == value || value.length == 0;
    }


    public static boolean isEmpty(final Collection<?> collection) {
        return null == collection || collection.size() == 0;
    }

    public static boolean isEmpty(final Date time) {
        return null == time || time.getTime() == 0;
    }

    public static boolean isEmpty(final int[] a) {
        return null == a || a.length == 0;
    }


    public static boolean isEmpty(final Integer[] a) {
        return null == a || a.length == 0;
    }


    public static boolean isEmpty(final long[] a) {
        return null == a || a.length == 0;
    }

    public static boolean isEmpty(final Long[] a) {
        return null == a || a.length == 0;
    }


    public static boolean isEmpty(final Map<?, ?> map) {
        return null == map || map.size() == 0;
    }

    public static boolean isEmpty(final Object[] a) {
        return null == a || a.length == 0;
    }


    public static boolean isEmpty(final String s) {
        return null == s || s.length() == 0;
    }


    public static boolean isEmpty(final String[] a) {
        return null == a || a.length == 0;
    }


    public static boolean notEmpty(final boolean[] value) {
        return null != value && value.length > 0;
    }

    public static boolean notEmpty(final byte[] value) {
        return null != value && value.length > 0;
    }


    public static boolean notEmpty(final char[] value) {
        return null != value && value.length > 0;
    }


    public static boolean notEmpty(final CharSequence s) {
        return null != s && s.length() > 0;
    }

    public static boolean notEmpty(final CharSequence[] value) {
        return null != value && value.length > 0;
    }


    public static boolean notEmpty(final Collection<?> collection) {
        return null != collection && collection.size() > 0;
    }

    public static boolean notEmpty(final Date time) {
        return null != time;
    }


    public static boolean notEmpty(final int[] a) {
        return null != a && a.length > 0;
    }


    public static boolean notEmpty(final Integer[] a) {
        return null != a && a.length > 0;
    }

    public static boolean notEmpty(final long[] a) {
        return null != a && a.length > 0;
    }

    public static boolean notEmpty(final Long[] a) {
        return null != a && a.length > 0;
    }


    public static boolean notEmpty(final Map<?, ?> map) {
        return null != map && map.size() > 0;
    }


    public static boolean notEmpty(final Object[] value) {
        return null != value && value.length > 0;
    }


    public static boolean notEmpty(final String s) {
        return null != s && s.length() > 0;
    }


    public static boolean notEmpty(final String[] a) {
        return null != a && a.length > 0;
    }

    private CheckUtil() {

    }
}
