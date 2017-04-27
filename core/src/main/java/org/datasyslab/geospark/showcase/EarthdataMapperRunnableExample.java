package org.datasyslab.geospark.showcase;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.formatMapper.EarthdataHDFPointMapper;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;

import com.vividsolutions.jts.geom.Envelope;

public class EarthdataMapperRunnableExample {

    /** The sc. */
    public static JavaSparkContext sc;
    
    /** The Input location. */
    static String InputLocation;

    
    /** The splitter. */
    static FileDataSplitter splitter;
    
    /** The index type. */
    static IndexType indexType;
    
    /** The num partitions. */
    static Integer numPartitions;
    
    /** The query envelope. */
    static Envelope queryEnvelope;
    
    /** The loop times. */
    static int loopTimes;
    
    static int HDFincrement = 5;
    
    static int HDFoffset = 2;
    
    static String HDFrootGroupName = "MOD_Swath_LST";
    
    static String HDFDataVariableName = "LST";
    
    static String urlPrefix = "";
	
	public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("EarthdataMapperRunnableExample").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        InputLocation = System.getProperty("user.dir")+"/src/test/resources/modis/modis.csv";
        splitter = FileDataSplitter.CSV;
        indexType = IndexType.RTREE;
        queryEnvelope=new Envelope (-90.01,-80.01,30.01,40.01);
        numPartitions = 5;
        loopTimes=1;
        HDFincrement=5;
        HDFoffset=2;
        HDFrootGroupName = "MOD_Swath_LST";
        HDFDataVariableName = "LST";
        urlPrefix = System.getProperty("user.dir")+"/src/test/resources/modis/";
        testSpatialRangeQuery();
        testSpatialRangeQueryUsingIndex();
	}

    public static void testSpatialRangeQuery() {
    	EarthdataHDFPointMapper earthdataHDFPoint = new EarthdataHDFPointMapper(HDFincrement,HDFoffset,HDFrootGroupName,
    			HDFDataVariableName,urlPrefix);
    	PointRDD spatialRDD = new PointRDD(sc, InputLocation, numPartitions, earthdataHDFPoint,StorageLevel.MEMORY_ONLY());
    	for(int i=0;i<loopTimes;i++)
    	{
    		long resultSize;
			try {
				resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false,false).count();
	    		assert resultSize>0;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}        
    }

    public static void testSpatialRangeQueryUsingIndex() {
    	EarthdataHDFPointMapper earthdataHDFPoint = new EarthdataHDFPointMapper(HDFincrement,HDFoffset,HDFrootGroupName,
    			HDFDataVariableName,urlPrefix);
    	PointRDD spatialRDD = new PointRDD(sc, InputLocation, numPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY());
    	try {
			spatialRDD.buildIndex(IndexType.RTREE,false);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	for(int i=0;i<loopTimes;i++)
    	{
			try {
	    		long resultSize;
				resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false,true).count();
	    		assert resultSize>0;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    }
}
