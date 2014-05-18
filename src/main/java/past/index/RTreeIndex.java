package past.index;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.khelekore.prtree.DistanceResult;
import org.khelekore.prtree.PRTree;

import scala.Tuple2;

public class RTreeIndex implements DatabaseIndex<NamedVector> {

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
	 * partitionNumber is 1-based.
	 */
	private JavaPairRDD<Integer, PRTree<NamedVector>> trees;
	
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
	 * @param vector vector of point to which compute neighbors.
	 * 
	 * @return the numberOfNeighbors nearest neighbors.
	 */
	@Override
	public List<NamedVector> nearestNeighbors(int numberOfNeighbors, NamedVector vector) {
		
		if (numberOfNeighbors < 1 || vector == null) {
			throw new IllegalArgumentException();
		}
		
		if (!this.isBuilt) {
			throw new IllegalStateException("The index has not been initialized.");
		}

        List<List<DistanceResult<NamedVector>>> nearestNeighborsVectors = this.trees.
                flatMap(new NearestNeighborsMapper(numberOfNeighbors, vector.getOrds())). // retrieve NN for each trees
                glom(). // put everything in a list
                collect(); // get back the result

		return this.sortAndSelectNeighbors(nearestNeighborsVectors.get(0), numberOfNeighbors);
	}
	
	/**
	 * Based on the return value of a PRTree nearestNeighbors, this method sorts the
	 * neighbors according to the distance and returns a sorted list of the neighboring vectors.
	 * 
	 * @param nearestNeighborsVectors the neighboring vector
	 * 
	 * @return the sorted list of neighboring vectors.
	 */
	private List<NamedVector> sortAndSelectNeighbors(List<DistanceResult<NamedVector>> nearestNeighborsVectors,
			int numberOfNeighbors) {

        Comparator<DistanceResult<NamedVector>> comparator = new Comparator<DistanceResult<NamedVector>>() {
            @Override
            public int compare(DistanceResult<NamedVector> o1, DistanceResult<NamedVector> o2) {
                return Double.compare(o1.getDistance(), o2.getDistance());
            }
        };

        List<NamedVector> sortedVectors = new ArrayList<NamedVector>();
        Collections.sort(nearestNeighborsVectors, comparator);

        for (DistanceResult<NamedVector> dr : nearestNeighborsVectors) {
            sortedVectors.add(dr.get());
        }

        // filters the unwanted neighbors
        if (numberOfNeighbors < sortedVectors.size()) {
            sortedVectors.subList(numberOfNeighbors, sortedVectors.size()).clear();
        }

        return sortedVectors;
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
		
		List<Tuple2<BigInteger, Integer>> localSortedValues = 
				conf.getDataset().
				sample(false, this.conf.getSampleFraction(), new Random().nextInt()). // taking samples
				map(new ZCurveValuesMapper(this.conf)). // mapping z-order curve to sampled vector
				sortByKey(true). // sorting values
				collect(); // sampled vectors should be small enough

		int partitionStep = localSortedValues.size() / this.conf.getNumberOfPartitions();
        if (localSortedValues.size() % this.conf.getNumberOfPartitions() != 0) {
            partitionStep++;
        }

		for (int i = 0 ; i < this.splittingPoints.length ; i++) {
			this.splittingPoints[i] = localSortedValues.get((i+1) * partitionStep - 1)._1();
        }
	}
	
	/**
	 * Map-Reduce algorithm to build the RTrees. They are then kept separate.
	 * The reference to the RTrees are then stored.
	 */
	private void buildRTrees() {
		
		// maps each partition to its pointer
		JavaPairRDD<Integer, NamedVector> vectorsByPartitions = 
				conf.getDataset().map(new VectorToPartitionMapper(conf, this.splittingPoints));
		
		// groups all pointers by key
		JavaPairRDD<Integer, List<NamedVector>> vectorsLists = vectorsByPartitions.groupByKey();
		
		// create and store RTrees
		this.trees = vectorsLists.map(new RTreesMapper(conf));
	}
	
	public boolean isBuilt() {
		return this.isBuilt;
	}
	
	//-----------TESTING METHODS --- TO REMOVE
	public void estimateSplittingPointsTest() {
		this.estimateSplittingPoints();
	}
	public void builRTreesTest() {
		this.buildRTrees();
	}
	public BigInteger[] getSplittingPoints() {
		return this.splittingPoints;
	}
	public JavaPairRDD<Integer, PRTree<NamedVector>> getTrees() {
		return this.trees;
	}
}
