package past.index;

import java.math.BigInteger;
import java.util.List;
import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.khelekore.prtree.PRTree;
import org.khelekore.prtree.PointND;

import scala.Tuple2;

public class RTreeIndex {
	

	/**
	 * The dataset is timeseries. Example if the dimension is 4:
	 * {28, 920, 983, 220}
	 * {49, 853, 122, 7453}
	 * 
	 * The RDD is represented as a pair :
	 * pointer -> vector
	 */
	private JavaPairRDD<Integer, Integer[]> dataset;
	private boolean isBuilt;
	private RTreeIndexConf conf;
	
	/**
	 * Splitting points of the partitions. Is always sorted.
	 * We have : Curve(P1...) <= SplitPoint(R1) < Curve(P2...) <= SplitPoint(R2) ...
	 */
	private BigInteger[] splittingPoints;
	
	/**
	 * 
	 * @param dataset
	 */
	public RTreeIndex(JavaPairRDD<Integer, Integer[]> dataset, RTreeIndexConf conf) {
		if (dataset == null || conf == null) {
			throw new IllegalArgumentException();
		}
		
		this.dataset = dataset;
		this.conf = conf;
		
		this.isBuilt = false;
		this.splittingPoints = new BigInteger[conf.getNumberOfPartitions() - 1];
	}
	
	/**
	 * Builds the R-Tree index.
	 */
	public void buildIndex() {
		
		this.estimateSplittingPoints();
		this.buildRTree();
		this.isBuilt = true;
	}
	
	/**
	 * Map-Reduce algorithm to estimate the splitting points of the dataset.
	 */
	private void estimateSplittingPoints() {
		
		// first taking samples of vectors of the dataset
		JavaPairRDD<Integer, Integer[]> sample = dataset.sample(false, this.conf.getSampleFraction(), new Random().nextInt());
		
		// mapping the z-order curve to each sampled vector
		JavaPairRDD<BigInteger, Integer> spaceFillingValues = sample.map(new ZCurveValuesMapper(this.conf));
		
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
	
	private void buildRTree() {
		// maps each partition to its pointer
		JavaPairRDD<Integer, Integer> vectorsByPartitions = this.dataset.map(
				new VectorToPartitionMapper(conf, this.splittingPoints));
		
		// groups all pointers by key
		JavaPairRDD<Integer, List<Integer>> vectorsLists = vectorsByPartitions.groupByKey();
		
		// create RTrees
		JavaPairRDD<Integer, PRTree<PointND>> trees = vectorsLists.map(new RTreesMapper(conf));
		// TODO to be finished
		
	}
	

	
	public boolean isBuilt() {
		return this.isBuilt;
	}
}
