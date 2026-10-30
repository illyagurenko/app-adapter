package ru.itone.illya4gurenko.utils;

/**
 * утилитный класс для удобного перевода единиц памяти (байт) и времени (миллисекунд).
 */
public final class TimeAndSizeUtils {

    private TimeAndSizeUtils() {
    }

    /**
     * переводит значение в килобайты (байты).
     */
    public static long kb(long value) {
        return value * 1024L;
    }

    /**
     * переводит значение в мегабайты (байты).
     */
    public static long mb(long value) {
        return kb(value) * 1024L;
    }

    /**
     * переводит значение в гигабайты (байты).
     */
    public static long gb(long value) {
        return mb(value) * 1024L;
    }

    /**
     * переводит секунды в миллисекунды.
     */
    public static long seconds(long value) {
        return value * 1_000L;
    }

    /**
     * переводит минуты в миллисекунды.
     */
    public static long minutes(long value) {
        return seconds(value * 60L);
    }

    /**
     * переводит часы в миллисекунды.
     */
    public static long hours(long value) {
        return minutes(value * 60L);
    }

    /**
     * переводит дни в миллисекунды.
     */
    public static long days(long value) {
        return hours(value * 24L);
    }
}