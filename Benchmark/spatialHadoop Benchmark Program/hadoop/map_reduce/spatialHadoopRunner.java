package hadoop.map_reduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ProgramDriver;

import edu.umn.cs.spatialHadoop.operations.*;
import edu.umn.cs.spatialHadoop.RandomSpatialGenerator;
import edu.umn.cs.spatialHadoop.ReadFile;
import edu.umn.cs.spatialHadoop.indexing.Indexer;
import edu.umn.cs.spatialHadoop.nasa.HDFPlot;
import edu.umn.cs.spatialHadoop.nasa.HDFToText;
import edu.umn.cs.spatialHadoop.nasa.ShahedServer;

public class spatialHadoopRunner {
	public static int spatialOperation(String args) throws Exception{
		int exitCode = 0;
		ProgramDriver pgd = new ProgramDriver();
		String[] args1 = args.split(" ");
		try {
			pgd.addClass("rangequery", RangeQuery.class,
					"Finds all objects in the query range given by a rectangle");

			pgd.addClass("knn", KNN.class,
					"Finds the k nearest neighbor in a file to a point");

			pgd.addClass("dj", DistributedJoin.class,
					"Computes the spatial join between two input files using the "
							+ "distributed join algorithm");

			pgd.addClass("sjmr", SJMR.class,
					"Computes the spatial join between two input files using the "
							+ "SJMR algorithm");

			pgd.addClass("index", Repartition.class,
					"Builds an index on an input file");

			pgd.addClass("partition", Indexer.class,
					"Spatially partition a file using a specific partitioner");

			pgd.addClass("mbr", FileMBR.class,
					"Finds the minimal bounding rectangle of an input file");

			pgd.addClass("readfile", ReadFile.class,
					"Retrieve some information about the index of a file");

			pgd.addClass("sample", Sampler.class,
					"Reads a random sample from the input file");

			pgd.addClass("generate", RandomSpatialGenerator.class,
					"Generates a random file containing spatial data");

			pgd.addClass("hdfplot", HDFPlot.class,
					"Plots a heat map for a give NASA dataset");

			pgd.addClass("gplot", GeometricPlot.class,
					"Plots a file to an image");

			pgd.addClass("hplot", HeatMapPlot.class,
					"Plots a heat map to an image");

			pgd.addClass("hdfx", HDFToText.class,
					"Extracts data from a set of HDF files to text files");

			pgd.addClass("skyline", Skyline.class,
					"Computes the skyline of an input set of points");

			pgd.addClass("convexhull", ConvexHull.class,
					"Computes the convex hull of an input set of points");

			pgd.addClass("farthestpair", FarthestPair.class,
					"Computes the farthest pair of point of an input set of points");

			pgd.addClass("closestpair", ClosestPair.class,
					"Computes the closest pair of point of an input set of points");

			pgd.addClass("distcp", DistributedCopy.class,
					"Copies a directory or file using a MapReduce job");

			pgd.addClass("vizserver", ShahedServer.class,
					"Starts a server that handles visualization requests");

			pgd.driver(args1);
			
			return exitCode;
		} catch (Throwable e) {
			exitCode=1;
			e.printStackTrace();
		}
		return exitCode;
	}
}
