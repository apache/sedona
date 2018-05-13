package org.datasyslab.geospark.spatioTemporal.RDD;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.datasyslab.geospark.SpatioTemporalObjects.Point3D;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;

public class DataDistributionTest {

    static InputStream input;

    static String dir;

    @BeforeClass
    public static void before() {
        ClassLoader classLoader = DataDistributionTest.class.getClassLoader();
        input = classLoader.getResourceAsStream("Gowalla_totalCheckins.txt");
        dir = System.getProperty("user.dir") + "\\src\\test\\resources";
    }

    @Test
    public void dataDistributionTest()
            throws Exception
    {
        List<Point3D> result = new ArrayList<>();
        GeometryFactory factory = new GeometryFactory();
        String fileName = dir + "\\Gowalla_totalCheckins.txt";
        BufferedReader reader=new BufferedReader(new FileReader(fileName));
        String line = null;
        while((line=reader.readLine())!=null){
            String[] columns = line.split("\\t");
            String time = columns[1];
            time = time.replace("T", " ");
            time = time.replace("Z", "");
            DateFormat format =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            double z;
            try {
                Date date = format.parse(time);
                z = date.getTime();
            } catch (java.text.ParseException e) {
                e.printStackTrace();
                throw new ParseException(e);
            }
            Coordinate coordinate = new Coordinate(
                    Double.parseDouble(columns[2]),
                    Double.parseDouble(columns[3]));
            Point point = factory.createPoint(coordinate);

            Point3D point3D = new Point3D(point, z);
            result.add(point3D);
        }
        reader.close();

        int partitionNum = 8;
        sortList(result);
        double minTime = result.get(0).getZ();
        double maxTime = result.get(result.size() - 1).getZ();
        double interval = (maxTime - minTime) / partitionNum;

        ArrayList<ArrayList<Point3D>> parts = new ArrayList<ArrayList<Point3D>>();
        for (int i = 0; i < partitionNum; i++) {
            parts.add(new ArrayList<Point3D>());
        }

        for (int i = 0; i < result.size(); i++) {
            int index = (int) ((result.get(i).getZ() - minTime) / interval);
            if (index >= partitionNum) {
                index = partitionNum - 1;
            }
            parts.get(index).add(result.get(i));
        }

        for (int i = 0; i < partitionNum; i++) {
            double minz = parts.get(i).get(0).getZ();
            double maxz = parts.get(i).get(parts.get(i).size() - 1).getZ();
            String begin = double2String(minz);
            String end = double2String(maxz);

            System.out.println(begin + " " + end + " " + parts.get(i).size());
        }
    }

    private String double2String(double min) {
        Date minDate = new Date((long) min);
        String minTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(minDate);
        return minTime;
    }

    private void sortList(List<Point3D> sample) {
        Collections.sort(sample, new Comparator<Point3D>() {
            @Override
            public int compare(Point3D cube1, Point3D cube2)
            {
                int res = 0;
                if (cube1.getZ() < cube2.getZ()) {
                    res = -1;
                } else if (cube1.getZ() == cube2.getZ()) {
                    res = 0;
                } else if (cube1.getZ() > cube2.getZ()) {
                    res = 1;
                }
                return  res;
            }
        });
    }

}
