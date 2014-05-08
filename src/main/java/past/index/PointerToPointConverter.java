package past.index;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.khelekore.prtree.MBRConverter;

class PointerToPointConverter implements MBRConverter<Integer> {
	private int dimension;
	private JavaPairRDD<Integer, Integer[]> dataset;
	
	PointerToPointConverter(int dimension, JavaPairRDD<Integer, Integer[]> dataset) {
		this.dimension = dimension;
		this.dataset = dataset;
	}
	
	@Override
	public int getDimensions() {
		return this.dimension;
	}

	@Override
	public double getMax(int axis, Integer pointer) {
		List<Integer[]> list = this.dataset.lookup(pointer);
		
		if (list.size() != 1) {
			throw new IllegalStateException("The pointer points to a number != 1 of values, this is illegal.");
		}
		
		return list.get(0)[axis];
	}

	@Override
	public double getMin(int axis, Integer pointer) {
		return this.getMax(axis, pointer);
	}
}
