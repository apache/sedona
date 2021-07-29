package org.apache.sedona.core.io.parquet;

import com.clearspring.analytics.util.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.sedona.core.TestBase;
import org.apache.sedona.core.enums.FileDataSplitter;
import org.apache.sedona.core.enums.GeometryType;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.formatMapper.ParquetReader;
import org.apache.sedona.core.formatMapper.WktReader;
import org.apache.sedona.core.formatMapper.WktReaderTest;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.util.Assert;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ParquetFileReaderTest extends TestBase {
    private static String filePath;
    @BeforeClass
    public static void onceExecutedBeforeAll()
            throws IOException
    {
        initialize(ParquetFileReaderTest.class.getName());
        filePath = ParquetFileReaderTest.class.getClassLoader().getResource("crs-test-point.csv").getPath();
    }
    
    @AfterClass
    public static void tearDown()
            throws Exception
    {
        sc.stop();
    }
    
    @Before
    public void delete() throws IOException {
        if(testOutputPathDir.exists()){
            FileUtils.deleteDirectory(testOutputPathDir);
        }
    }
    
    @Test
    public void testParquetReader() throws IOException, SedonaException {
        PointRDD pointRDD = new PointRDD(sc, filePath, 0, FileDataSplitter.CSV, false, 1);
        pointRDD.saveAsParquet(sc,
                               "p",
                               Lists.newArrayList(),
                               testOutputPathDir.getAbsolutePath(),
                               "test.namespace",
                               "name");
        List<Point> l1 = pointRDD.getRawSpatialRDD().collect().stream().collect(Collectors.toList());
        Collections.sort(l1, new Comparator<Point>() {
            @Override
            public int compare(Point o1, Point o2) {
                return o1.compareTo(o2);
            }
        });
        
        SpatialRDD<Point> pointRDD1 = ParquetReader.readToGeometryRDD(sc,
                                                                        Arrays.stream(testOutputPathDir.listFiles())
                                                                              .filter(file -> file.getAbsolutePath().endsWith(".parquet"))
                                                                              .map(file->file.getAbsolutePath())
                                                                              .collect(Collectors.toList()),
                                                                        GeometryType.POINT,
                                                                        "p",
                                                                        Collections.EMPTY_LIST);
        List<Point> l2 = pointRDD1.getRawSpatialRDD().collect().stream().collect(Collectors.toList());
        Collections.sort(l2, new Comparator<Point>() {
            @Override
            public int compare(Point o1, Point o2) {
                return o1.compareTo(o2);
            }
        });
        Assert.equals(l1,l2);
    }
}
