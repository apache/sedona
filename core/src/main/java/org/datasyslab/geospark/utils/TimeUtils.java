package org.datasyslab.geospark.utils;

public class TimeUtils
{

    public static long elapsedSince(long startTime)
    {
        return System.currentTimeMillis() - startTime;
    }
}
