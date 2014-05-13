package past.index;

import java.util.List;

import org.apache.spark.api.java.function.PairFunction;
import org.khelekore.prtree.PRTree;

import scala.Tuple2;

public class RTreesMapper extends PairFunction<
											Tuple2<Integer, List<NamedVector>>, 
											Integer, 
											PRTree<NamedVector>> {

	private static final long serialVersionUID = -5584814519663194688L;
	private RTreeIndexConf conf;
	
	public RTreesMapper(RTreeIndexConf conf) {
		this.conf = conf;
	}
	
	/**
	 * Generates RTrees for each partition.
	 * 
	 * @param partitionNmbrAndVector tuple : (partitionNumber, list of named vectors)
	 * 
	 * @return tuple : (partitionNumber, RTree of named vectors)
	 */
	@Override
	public Tuple2<Integer, PRTree<NamedVector>> call(
			Tuple2<Integer, List<NamedVector>> partitionNmbrAndVector) throws Exception {

		PRTree<NamedVector> tree = new PRTree<NamedVector>(
				new NamedVectorConverter(conf), conf.getRTreeBranchFactor());
		
		tree.load(partitionNmbrAndVector._2());
		return new Tuple2<Integer, PRTree<NamedVector>>(partitionNmbrAndVector._1(), tree);
	}

}
