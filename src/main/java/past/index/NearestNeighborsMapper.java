package past.index;

import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.khelekore.prtree.DistanceCalculator;
import org.khelekore.prtree.DistanceResult;
import org.khelekore.prtree.NodeFilter;
import org.khelekore.prtree.PRTree;
import org.khelekore.prtree.PointND;
import org.khelekore.prtree.SimplePointND;

import scala.Tuple2;

public class NearestNeighborsMapper extends FlatMapFunction<Tuple2<Integer, PRTree<Integer>>, DistanceResult<Integer>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8371370813617201740L;
	private RTreeIndexConf conf;
	private int numberOfNeighbors;
	private double[] point;
	
	

	public NearestNeighborsMapper(int numberOfNeighbors, Integer[] point, RTreeIndexConf conf) {
		this.numberOfNeighbors = numberOfNeighbors;
		this.conf = conf;
		
		this.point = new double[point.length];
		for (int i = 0 ; i < point.length ; i++) {
			this.point[i] = point[i].doubleValue();
		}
	}

	@Override
	public Iterable<DistanceResult<Integer>> call(Tuple2<Integer, PRTree<Integer>> partitionAndTree) throws Exception {
		PRTree<Integer> tree = partitionAndTree._2();
		
		// no filtering
		NodeFilter<Integer> filter = new NodeFilter<Integer>() {
			public boolean accept(Integer node) {
				return true;
			}
		};
		
		List<DistanceResult<Integer>> distResults = tree.nearestNeighbour(
				new PointDistanceCalculator(this.conf), 
				filter, 
				this.numberOfNeighbors, 
				new SimplePointND(this.point));
		
		return distResults;
	}

}

class PointDistanceCalculator implements DistanceCalculator<Integer> {
	
	private RTreeIndexConf conf;
	
	PointDistanceCalculator(RTreeIndexConf conf) {
		this.conf = conf;
	}
	
	/**
	 * Computes the distance between 2 points.
	 * 
	 * @param p pointer to the point
	 * @param t pointer to the vector 
	 */
	@Override
	public double distanceTo(Integer pointer, PointND point2) {
		List<Integer[]> valuesOfKey = this.conf.getDataset().lookup(pointer);
		if (valuesOfKey.size() != 1) {
			throw new AssertionError("There can be only one value associated to a key in the dataset.");
		}
		
		Integer[] point1 = valuesOfKey.get(0);
		
		if (point2.getDimensions() != point1.length) {
			throw new AssertionError("Dimensions of the 2 points are not the same.");
		}
		
		int tmpSum = 0;
		for (int axis = 0 ; axis < point2.getDimensions() ; axis++) {
			tmpSum += Math.pow(point2.getOrd(axis) - point1[axis], 2);
		}
		
		return Math.sqrt(tmpSum);
	}
}
