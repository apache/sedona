package org.apache.sedona.sql.raster;


public class Operations {

    public Operations() {}

    public double[] NDVI(double[] band1, double[] band2)
    {
        double[] result = new double[band1.length];
        for(int i = 0;i < band1.length; i++)
        {
            band1[i] = band1[i]==0?-1:band1[i];
            band2[i] = band2[i]==0?-1:band2[i];
        }

        for(int i = 0;i<band1.length;i++)
        {
            result[i] = (band2[i] - band1[i])/(band1[i] + band2[i]);
        }

        return result;
    }




}

