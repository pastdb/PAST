package past.index;

import java.util.List;

import org.apache.spark.api.java.function.PairFunction;
import org.khelekore.prtree.PRTree;

import scala.Tuple2;

public class RTreesMapper extends PairFunction<
											Tuple2<Integer, List<Integer>>, 
											Integer, 
											PRTree<Integer>> {

	private static final long serialVersionUID = -5584814519663194688L;
	private RTreeIndexConf conf;
	
	public RTreesMapper(RTreeIndexConf conf) {
		this.conf = conf;
	}
	
	/**
	 * Generates RTrees for each partition.
	 * 
	 * @param partitionNmbrAndPointer tuple : (partitionNumber, list of pointers to vectors)
	 * 
	 * @return tuple : (partitionNumber, RTree of pointers to vector 
	 */
	@Override
	public Tuple2<Integer, PRTree<Integer>> call(
			Tuple2<Integer, List<Integer>> partitionNmbrAndPointer) throws Exception {
		
		PRTree<Integer> tree = new PRTree<Integer>(
				new PointerToPointConverter(
						conf.getDataDimension(), conf.getDataset()), conf.getRTreeBranchFactor());
		
		tree.load(partitionNmbrAndPointer._2());
		return new Tuple2<Integer, PRTree<Integer>>(partitionNmbrAndPointer._1(), tree);
	}

}
