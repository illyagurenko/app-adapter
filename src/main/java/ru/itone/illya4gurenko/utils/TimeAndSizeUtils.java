package ru.itone.illya4gurenko.utils;

public final class TimeAndSizeUtils {

    private TimeAndSizeUtils() {
    }

    public static long kb(long value) {
        return value * 1024L;
    }

    public static long mb(long value) {
        return kb(value) * 1024L;
    }

    public static long gb(long value) {
        return mb(value) * 1024L;
    }

    public static long seconds(long value) {
        return value * 1_000L;
    }

    public static long minutes(long value) {
        return seconds(value * 60L);
    }

    public static long hours(long value) {
        return minutes(value * 60L);
    }

    public static long days(long value) {
        return hours(value * 24L);
    }
}