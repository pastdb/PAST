package past.index;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public abstract class VectorMapper<K, V> extends PairFunction<Tuple2<String, int[]>, K, V> {

	private static final long serialVersionUID = -2716903251106840411L;
	
	protected RTreeIndexConf conf;
	
	public VectorMapper(RTreeIndexConf conf) {
		this.conf = conf;
	}
	
	/**
	 * Computes the z-curve value of a given vector.
	 * 
	 * @param vector the vector to which compute the z-curve value
	 * @return the z-curve values
	 */
	protected BigInteger computeZCurveValue(int[] vector) {
		// 4 = number of bytes (8 bits) in a java integer (32 bits)
		ByteBuffer buffer = ByteBuffer.allocate(this.conf.getDataDimension() * 4);
		
		for (int i = 0 ; i < this.conf.getDataDimension() ; i++) {
			buffer.putInt(i * 4, vector[i]); // 
		}
		
		return new BigInteger(buffer.array());
	}
}
