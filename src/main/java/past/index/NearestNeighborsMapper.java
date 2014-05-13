package past.index;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.khelekore.prtree.DistanceCalculator;
import org.khelekore.prtree.DistanceResult;
import org.khelekore.prtree.NodeFilter;
import org.khelekore.prtree.PRTree;
import org.khelekore.prtree.PointND;
import org.khelekore.prtree.SimplePointND;

import scala.Tuple2;

public class NearestNeighborsMapper extends FlatMapFunction<Tuple2<Integer, PRTree<NamedVector>>, DistanceResult<NamedVector>> {

	private int numberOfNeighbors;
	private double[] point;
	
	public NearestNeighborsMapper(int numberOfNeighbors, int[] point) {
		this.numberOfNeighbors = numberOfNeighbors;
		
		this.point = new double[point.length];
		for (int i = 0 ; i < point.length ; i++) {
			this.point[i] = (double) point[i];
		}
	}

	@Override
	public Iterable<DistanceResult<NamedVector>> call(Tuple2<Integer, PRTree<NamedVector>> partitionAndTree) 
			throws Exception {
		
		PRTree<NamedVector> tree = partitionAndTree._2();
		
		// no filtering
		NodeFilter<NamedVector> filter = new NodeFilter<NamedVector>() {
			public boolean accept(NamedVector node) {
				return true;
			}
		};

        return tree.nearestNeighbour(
                new PointDistanceCalculator(),
                filter,
                this.numberOfNeighbors,
                new SimplePointND(this.point));
	}
}

class PointDistanceCalculator implements DistanceCalculator<NamedVector> {
	
	/**
	 * Computes the distance between 2 points.
	 * 
	 * @param pointer first point
	 * @param point2 second point
	 */
	@Override
	public double distanceTo(NamedVector pointer, PointND point2) {

		int[] point1 = pointer.getOrds();
		
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
