package org.apache.sedona.sql.raster;

import org.geotools.coverage.grid.GridCoordinates2D;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.OverviewPolicy;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.opengis.coverage.grid.GridCoordinates;
import org.opengis.coverage.grid.GridEnvelope;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValue;
import org.opengis.referencing.operation.TransformException;
import org.geotools.geometry.Envelope2D;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class Construction {

    public Construction() {}

    public String getBands(String line) throws IOException, TransformException {

        ParameterValue<OverviewPolicy> policy = AbstractGridFormat.OVERVIEW_POLICY.createValue();
        policy.setValue(OverviewPolicy.IGNORE);

        ParameterValue<String> gridsize = AbstractGridFormat.SUGGESTED_TILE_SIZE.createValue();

        //Setting read type: use JAI ImageRead (true) or ImageReaders read methods (false)
        ParameterValue<Boolean> useJaiRead = AbstractGridFormat.USE_JAI_IMAGEREAD.createValue();
        useJaiRead.setValue(true);


        GridCoverage2DReader reader = new GeoTiffReader(line);
        GridCoverage2D coverage = reader.read(
                new GeneralParameterValue[]{policy, gridsize, useJaiRead}
        );

        GridGeometry2D geometry = coverage.getGridGeometry();
        GridEnvelope dimensions = reader.getOriginalGridRange();
        GridCoordinates maxDimensions = dimensions.getHigh();
        int w = maxDimensions.getCoordinateValue(0)+1;
        int h = maxDimensions.getCoordinateValue(1)+1;
        int numBands = reader.getGridCoverageCount();
        System.out.println(numBands);
        numBands = 4;

        List<List<Double>> bandValues = new ArrayList<>(numBands);

        for(int i=0;i<numBands;i++)
            bandValues.add(new ArrayList<>());

        for (int i=0; i<w; i++) {
            for (int j=0; j<h; j++) {

                Envelope2D pixelEnvelop =
                        geometry.gridToWorld(new GridEnvelope2D(i, j, 1, 1));

                double[] vals = new double[numBands];
                coverage.evaluate(new GridCoordinates2D(i, j), vals);

                int band = 0;
                for(double pixel:vals)
                {
                    bandValues.get(band++).add(pixel);
                }



            }
        }
        return toString(bandValues);

    }

    private String toString(List<List<Double>> input)
    {

        StringBuilder sb = new StringBuilder();

        for(List<Double> l:input)
        {
            String tempList = l.stream().map(i->i.toString()).collect(Collectors.joining(", "));

            sb.append(tempList + ":");

        }
        String result = sb.toString();
        return result.substring(0,result.length()-1);

    }


}
