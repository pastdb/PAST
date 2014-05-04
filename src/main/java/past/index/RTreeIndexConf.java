package past.index;

public class RTreeIndexConf {
	
	/**
	 * Constant value used by the mappers.
	 */
	public static final int SINGLE_VALUE = 1;
	
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
	
	public RTreeIndexConf(int dataDimension, int nbPartitions, double sampleFraction, int rTreeBranchFactor) {
		if (dataDimension < 1 || nbPartitions < 1 || sampleFraction < 0 || rTreeBranchFactor < 1) {
			throw new IllegalArgumentException();
		}
		
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
	public RTreeIndexConf(int dataDimension) {
		this(dataDimension, 5, 0.03, 30);
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
}
