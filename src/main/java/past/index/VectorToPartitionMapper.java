package past.index;

import java.math.BigInteger;

import scala.Tuple2;

public class VectorToPartitionMapper extends VectorMapper<Integer, Integer> {
	
	private static final long serialVersionUID = 7567437116211474877L;
	private BigInteger[] splittingPoints;
	
	public VectorToPartitionMapper(RTreeIndexConf conf, BigInteger[] splittingPoints) {
		super(conf);
		this.splittingPoints = splittingPoints;
	}

	@Override
	public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer[]> vector) throws Exception {
		return new Tuple2<Integer, Integer>(
				this.computePartitionNumber(this.computeZCurveValue(vector._2())),
				vector._1());
	}
	
	/**
	 * Computes the partition number of a vector, using the splitting points defined earlier.
	 * 
	 * @param fillingSpaceValue the filling-space value of the vector we want to know its partition number.
	 * @return the partition number.
	 */
	private int computePartitionNumber(BigInteger fillingSpaceValue) {
		
		// testing the first and last partitions first, then for all the intermediates one
		if (fillingSpaceValue.compareTo(this.splittingPoints[0]) <= 0) {
			return 1;
		} else if (fillingSpaceValue.compareTo(this.splittingPoints[this.splittingPoints.length - 1]) > 0) {
			return this.conf.getNumberOfPartitions();
		} else {
			for (int i = 0 ; i < this.splittingPoints.length ; i++) {
				if (fillingSpaceValue.compareTo(this.splittingPoints[i]) > 0 &&
						fillingSpaceValue.compareTo(this.splittingPoints[i+1]) <= 0) {
					return i+2;
				}
			}
		}
		
		throw new IllegalStateException("Splitting points are not coherent.");
	}
}
