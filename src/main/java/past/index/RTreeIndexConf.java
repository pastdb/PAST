package past.index;

import org.apache.spark.api.java.JavaPairRDD;

public class RTreeIndexConf {
	
	/**
	 * Constant value used by the mappers.
	 */
	public static final int SINGLE_VALUE = 1;
	
	/**
	 * The dataset is timeseries. Example if the dimension is 4:
	 * {28, 920, 983, 220}
	 * {49, 853, 122, 7453}
	 * 
	 * The RDD is represented as a pair :
	 * pointer -> vector
	 */
	private JavaPairRDD<Integer, Integer[]> dataset;

	/**
	 * 
	 */
	private int dataDimension;
	
	/**
	 * The data will be partitioned by this number of partitions.
	 */
	private int numberOfPartitions;

	/**
	 * Relative quantity of timeseries vectors to sample for the estimation of the splitting points.
	 */
	private double sampleFraction;
	
	/**
	 * The branch factor of the R-Tree (=number of childs of each node).
	 */
	private int rTreeBranchFactor;

	
	public RTreeIndexConf(JavaPairRDD<Integer, Integer[]> dataset, int dataDimension, int nbPartitions, double sampleFraction, int rTreeBranchFactor) {
		if (dataset == null || dataDimension < 1 || nbPartitions < 1 || 
				sampleFraction < 0 || rTreeBranchFactor < 1) {
			throw new IllegalArgumentException();
		}
		
		this.dataset = dataset;
		this.dataDimension = dataDimension;
		this.numberOfPartitions = nbPartitions;
		this.sampleFraction = sampleFraction;
		this.rTreeBranchFactor = rTreeBranchFactor;
	}
	
	/**
	 * Uses the defined default values.
	 * 
	 * @param dataDimension
	 */
	public RTreeIndexConf(JavaPairRDD<Integer, Integer[]> dataset, int dataDimension) {
		this(dataset, dataDimension, 5, 0.03, 30);
	}

	public int getDataDimension() {
		return dataDimension;
	}
	
	public int getNumberOfPartitions() {
		return numberOfPartitions;
	}
	
	public double getSampleFraction() {
		return sampleFraction;
	}
	
	public int getRTreeBranchFactor() {
		return rTreeBranchFactor;
	}

	public JavaPairRDD<Integer, Integer[]> getDataset() {
		return this.dataset;
	}
}
