package past.index;

import java.util.List;

import org.apache.spark.api.java.function.PairFunction;
import org.khelekore.prtree.MBRConverter;
import org.khelekore.prtree.PRTree;
import org.khelekore.prtree.PointND;
import org.khelekore.prtree.SimplePointND;

import scala.Tuple2;

public class RTreesMapper extends PairFunction<
											Tuple2<Integer, List<Integer>>, 
											Integer, 
											PRTree<PointND>> {

	private static final long serialVersionUID = -5584814519663194688L;
	private RTreeIndexConf conf;
	
	public RTreesMapper(RTreeIndexConf conf) {
		this.conf = conf;
	}
	
	@Override
	public Tuple2<Integer, PRTree<PointND>> call(
			Tuple2<Integer, List<Integer>> partitionToVectors) throws Exception {
		
		PRTree<PointND> tree = new PRTree<PointND>(
				new PointNDConverter(this.conf.getDataDimension()), conf.getRTreeBranchFactor());
		
		
		/*
		tree.load(vectors);
		TODO : create new PointND containing both pointer and value ?
		
		PRTree<SimplePointND> tree = new
		
		
		return new Tuple2<Integer, PRTree<SimplePointND>>(partitionToVectors._1(), tree);*/
		return null;
	}

}

class PointNDConverter implements MBRConverter<PointND> {
	private int dimension;
	
	PointNDConverter(int dimension) {
		this.dimension = dimension;
	}
	
	@Override
	public int getDimensions() {
		return this.dimension;
	}

	@Override
	public double getMin(int axis, PointND t) {
		return t.getOrd(axis);
	}

	@Override
	public double getMax(int axis, PointND t) {
		return t.getOrd(axis);
	}
	
}
