package org.apache.sedona.snowflake.snowsql;

public class TypeUtils {

        public static long[] castLong(Long[] input) {
            long[] output = new long[input.length];
            for (int i = 0; i < input.length; i++) {
                output[i] = input[i];
            }
            return output;
        }

    public static Long[] toLong(long[] input) {
        Long[] output = new Long[input.length];
        for (int i = 0; i < input.length; i++) {
            output[i] = input[i];
        }
        return output;
    }
}
