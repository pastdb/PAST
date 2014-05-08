package past.index;

import java.math.BigInteger;

import scala.Tuple2;

/**
 * Maps each vector to a key/value pair.
 * Key = the vector's z-curve value.
 * Value = a constant.
 *
 * A constant is used for the value, in order to produce a JavaPairRDD where we can use sortByKey later.
 */
public class ZCurveValuesMapper extends VectorMapper<BigInteger, Integer> {
	
	private static final long serialVersionUID = -1581628164469445470L;
	
	public ZCurveValuesMapper(RTreeIndexConf conf) {
		super(conf);
	}

	@Override
	public Tuple2<BigInteger, Integer> call(Tuple2<Integer, Integer[]> sampledVector) throws Exception {
		return new Tuple2<BigInteger, Integer>(this.computeZCurveValue(sampledVector._2()), RTreeIndexConf.SINGLE_VALUE);
	}
}