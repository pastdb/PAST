package past.index;

import org.khelekore.prtree.MBRConverter;

import java.io.Serializable;

class NamedVectorConverter implements MBRConverter<NamedVector>, Serializable {
    private static final long serialVersionUID = 9116008266621740388L;
    private RTreeIndexConf conf;
	
	public NamedVectorConverter(RTreeIndexConf conf) {
		this.conf = conf;
	}

	@Override
	public int getDimensions() {
		return this.conf.getDataDimension();
	}

	@Override
	public double getMax(int axis, NamedVector vector) {
		return vector.getOrds()[axis];
	}

	@Override
	public double getMin(int axis, NamedVector vector) {
		return this.getMax(axis, vector);
	}
}
