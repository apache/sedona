package org.apache.sedona.core.utils;

import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;

import static org.junit.Assert.*;

public class SedonaConfTest {

    @BeforeClass
    public static void setUp() {
        SparkSession.builder().config("sedona.join.numpartition", "2").master("local").getOrCreate();

    }

    @AfterClass
    public static void tearDown() {
        SparkSession.active().sparkContext().stop();
    }

    @Test
    public void testRuntimeConf() {
        assertEquals(2, SedonaConf.fromActiveSession().getFallbackPartitionNum());
        SparkSession.active().conf().set("sedona.join.numpartition", "3");
        assertEquals(3, SedonaConf.fromActiveSession().getFallbackPartitionNum());
    }
    
    @Test
    public void testDatasetBoundary() {
        SparkSession.active().conf().set("sedona.join.boundary", "1,2,3,4");
        Envelope datasetBoundary = SedonaConf.fromActiveSession().getDatasetBoundary();
        assertEquals("Env[1.0 : 2.0, 3.0 : 4.0]", datasetBoundary.toString());
    }
}