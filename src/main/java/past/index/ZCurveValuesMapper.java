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
	
	public ZCurveValuesMapper(RTreeIndexConf conf) {
		super(conf);
	}

	private static final long serialVersionUID = -1581628164469445470L;
	

	@Override
	public Tuple2<BigInteger, Integer> call(Tuple2<String, int[]> sampledVector) throws Exception {
		return new Tuple2<BigInteger, Integer>(this.computeZCurveValue(sampledVector._2()), RTreeIndexConf.SINGLE_VALUE);
	}
}
