package past.index;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.khelekore.prtree.DistanceCalculator;
import org.khelekore.prtree.DistanceResult;
import org.khelekore.prtree.NodeFilter;
import org.khelekore.prtree.PRTree;
import org.khelekore.prtree.PointND;
import org.khelekore.prtree.SimplePointND;

import scala.Tuple2;
//TODO : add method to get vector from a pointer (new object to contain rdd)
public class RTreeIndex implements DatabaseIndex<Integer[]> {

	/**
	 * If the index is built or not.
	 */
	private boolean isBuilt;
	
	/**
	 * All the needed configuration.
	 */
	private RTreeIndexConf conf;
	
	/**
	 * Splitting points of the partitions. Is always sorted.
	 * We have : Curve(P1...) <= SplitPoint(R1) < Curve(P2...) <= SplitPoint(R2) ...
	 */
	private BigInteger[] splittingPoints;
	
	/**
	 * RTrees containing the index. 
	 * The pair is : (partitionNumber, RTree of pointers on vectors)
	 */
	private JavaPairRDD<Integer, PRTree<Integer>> trees;
	
	/**
	 * Default constructor.
	 * 
	 * @param conf the configuration of the index.
	 */
	public RTreeIndex(RTreeIndexConf conf) {
		if (conf == null) {
			throw new IllegalArgumentException();
		}
		
		this.conf = conf;
		
		this.isBuilt = false;
		this.splittingPoints = new BigInteger[conf.getNumberOfPartitions() - 1];
	}
	
	/**
	 * Query all RTrees for neirest neighbors, aggregate and filter the results.
	 * 
	 * @param numberOfNeighbors number of neighbors to compute.
	 * @param vector coordinates of point to which compute neighbors.
	 * 
	 * @return the numberOfNeighbors nearest neighbors.
	 */
	@Override
	public List<Integer[]> nearestNeighbors(int numberOfNeighbors, Integer[] vector) {
		
		if (numberOfNeighbors < 1 || vector == null) {
			throw new IllegalArgumentException();
		}
		
		if (!this.isBuilt) {
			throw new IllegalStateException("The index has not been initialized.");
		}
				
		JavaRDD<Iterable<DistanceResult<Integer>>> nearestNeighborsRDD = 
				this.trees.map(new NearestNeighborsMapper(numberOfNeighbors, vector, this.conf));
		
		Iterable<DistanceResult<Integer>> nearestNeighborsPointers =  nearestNeighborsRDD.collect().get(0);
		
		// creates hashmap containing distance -> pointer
		Map<Double, Integer> distanceToPointers = new HashMap<>();
		for (DistanceResult<Integer> nearestNeighborsPointer : nearestNeighborsPointers) {
			distanceToPointers.put(nearestNeighborsPointer.getDistance(), nearestNeighborsPointer.get());
		}
		
		//Collections.sort();
		//Double[] list = Arrays.asList(distanceToPointers.keySet().toArray());
		//to be finished
		return null;
	}
	
	/**
	 * Builds the R-Tree index.
	 */
	@Override
	public boolean buildIndex() {
		try {
			this.estimateSplittingPoints();
			this.buildRTrees();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		this.isBuilt = true;
		return true;
	}
	
	/**
	 * Map-Reduce algorithm to estimate the splitting points of the dataset.
	 * The splitting points are then stored.
	 */
	private void estimateSplittingPoints() {
		
		// first taking samples of vectors of the dataset
		JavaPairRDD<Integer, Integer[]> sample = 
				conf.getDataset().sample(false, this.conf.getSampleFraction(), new Random().nextInt());
		
		// mapping the z-order curve to each sampled vector
		JavaPairRDD<BigInteger, Integer> spaceFillingValues = 
				sample.map(new ZCurveValuesMapper(this.conf));
		
		// sorting the z-curve values
		JavaPairRDD<BigInteger, Integer> sortedValues = spaceFillingValues.sortByKey(true);
		
		// computing the splitting points
		Long numberOfValues = sortedValues.count();
		
		if (numberOfValues > Integer.MAX_VALUE) {
			// limitation due to the list.get(int) method used later
			throw new RuntimeException("The number of sample exceed the authorized quantity, please reduce the sampleFranction.");
		}
		
		int partitionStep = numberOfValues.intValue() / this.conf.getNumberOfPartitions() + 1;
		List<Tuple2<BigInteger, Integer>> localValues = sortedValues.collect(); // should be small enough
		
		for (int i = 0 ; i < this.splittingPoints.length ; i++) {
			this.splittingPoints[i] = localValues.get((i+1) * partitionStep - 1)._1();
		}
	}
	
	/**
	 * Map-Reduce algorithm to build the RTrees. They are then kept separate.
	 * The reference to the RTrees are then stored.
	 */
	private void buildRTrees() {
		// maps each partition to its pointer
		JavaPairRDD<Integer, Integer> vectorsByPartitions = 
				conf.getDataset().map(new VectorToPartitionMapper(conf, this.splittingPoints));
		
		// groups all pointers by key
		JavaPairRDD<Integer, List<Integer>> vectorsLists = vectorsByPartitions.groupByKey();
		
		// create and store RTrees
		this.trees = vectorsLists.map(new RTreesMapper(conf));
	}
	
	public boolean isBuilt() {
		return this.isBuilt;
	}
}
