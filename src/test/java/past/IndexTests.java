package past;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.khelekore.prtree.PRTree;
import org.khelekore.prtree.SimpleMBR;

import past.index.NamedVector;
import past.index.RTreeIndex;
import past.index.RTreeIndexConf;
import scala.Tuple2;
import static org.junit.Assert.*;

public class IndexTests {
	

    public static void main (String args[]) {
    	JUnitCore.main(IndexTests.class.getName());
    }
    
    private JavaPairRDD<String, int[]> generateDataset(JavaSparkContext jsc, int[][] vectors) {
		List<Tuple2<String, int[]>> rawData = new ArrayList<>();
		for (int i = 0 ; i < vectors.length ; i++) {
			rawData.add(new Tuple2<String, int[]>(i+"", vectors[i]));
		}
		
		return jsc.parallelizePairs(rawData);
    }

	@Test
	public void testSlittingPoints() {
        JavaSparkContext jsc = new JavaSparkContext("local", "dataset");

		int[][] splittingPointsvectors = new int[][] {
				{0, 0, 0, 0, 4}, //0
                {0, 0, 0, 0, 3}, //1
                {0, 0, 0, 0, 0}, //2
                {0, 0, 0, 0, 2}, //3
                {0, 0, 0, 0, 1}, //4
                {0, 0, 0, 0, 8}, //5
                {0, 0, 0, 0, 6}, //6
                {0, 0, 0, 0, 9}, //7
                {0, 0, 0, 0, 5}, //8
                {0, 0, 0, 0, 7}	 //9
			};
		
		JavaPairRDD<String, int[]> splittingPointsDataset = 
				this.generateDataset(jsc, splittingPointsvectors);
		
		//results should be 4 splitting points between each
		BigInteger[] expected = new BigInteger[] {
			new BigInteger("1"),
			new BigInteger("3"),
			new BigInteger("5"),
			new BigInteger("7")
        };
		
		RTreeIndexConf conf = new RTreeIndexConf(splittingPointsDataset, 5,
				5, // # of partitions
				1, // ratio of samples for splitting points
				3); // number of childs for each node of rtree
		
		RTreeIndex index = new RTreeIndex(conf);
		index.estimateSplittingPointsTest();
		
		assertArrayEquals(expected, index.getSplittingPoints());
	}

	@Test
	public void testRTrees() {
        JavaSparkContext jsc = new JavaSparkContext("local", "dataset2");

        int[][] rTreesVectors = new int[][] {
				{3, 3, 3, 3, 3},//0
				{5, 5, 5, 5, 5},//1
				{1, 1, 1, 1, 1},//2
				{4, 4, 4, 4, 4},//3
				{2, 2, 2, 2, 2},//4
				{1, 2, 3, 4, 5},//5
				{5, 4, 3, 2, 1},//6
				{0, 0, 0, 0, 0},//7
				{0, 1, 2, 3, 4},//8
				{4, 5, 2, 0, 4},//9
				{8, 1, 8, 1, 8},//10
				{9, 1, 8, 2, 6},//11
				{185, -8924, 17187, 9383, 333},	//12
				{2356, 7823, 8924, -12, 110}		//13
			};
		JavaPairRDD<String, int[]> rTreesDataset = 
				this.generateDataset(jsc, rTreesVectors);

		RTreeIndexConf conf = new RTreeIndexConf(rTreesDataset, 5,
				5, // # of partitions
				1, // ratio of samples for splitting points
				2); // number of childs for each node of rtree
		
		RTreeIndex index = new RTreeIndex(conf);
		index.buildIndex();
		
		JavaPairRDD<Integer, PRTree<NamedVector>> result = index.getTrees();
		
		assertEquals(5, result.count());
		
		// test of first tree, it should contain {0, 0, 0, 0, 0}//7 ; {0, 1, 2, 3, 4}//8 ; {1, 1, 1, 1, 1},//2
		{
			List<PRTree<NamedVector>> trees = result.lookup(1);
			assertEquals(1, trees.size());
			PRTree<NamedVector> tree = trees.get(0);
			assertEquals(3, tree.getNumberOfLeaves()); // 3 vectors by partitions
			assertEquals(2, tree.getHeight()); // branch factor = 2
			
			{
				Iterable<NamedVector> iterable = tree.find(new SimpleMBR(0, 1, 0, 1, 0, 2, 0, 3, 0, 4)); // 7, 8 and 2
				List<NamedVector> list = new ArrayList<>();
				for (NamedVector i : iterable) {
					list.add(i);
				}
				
				assertTrue(list.contains(new NamedVector("8", new int[]{0, 1, 2, 3, 4})));
				assertTrue(list.contains(new NamedVector("7", new int[]{0, 0, 0, 0, 0})));
				assertTrue(list.contains(new NamedVector("2", new int[]{1, 1, 1, 1, 1})));
				assertEquals(3, list.size());
			} {
				Iterable<NamedVector> iterable = tree.find(new SimpleMBR(0, 2, 0, 2, 0, 2, 0, 2, 0, 2)); // 7 and 2
				List<NamedVector> list = new ArrayList<>();
				for (NamedVector i : iterable) {
					list.add(i);
				}
				
				assertTrue(list.contains(new NamedVector("7", new int[]{0, 0, 0, 0, 0})));
				assertTrue(list.contains(new NamedVector("2", new int[]{1, 1, 1, 1, 1})));
				assertEquals(2, list.size());
			} {
				Iterable<NamedVector> iterable = tree.find(new SimpleMBR(0, 0, 1, 9, 0, 9, 0, 9, 0, 9)); // 8 only
				List<NamedVector> list = new ArrayList<>();
				for (NamedVector i : iterable) {
					list.add(i);
				}
				
				assertTrue(list.contains(new NamedVector("8", new int[]{0, 1, 2, 3, 4})));
				assertEquals(1, list.size()); 
			} {
				Iterable<NamedVector> iterable = tree.find(new SimpleMBR(9, 10, 9, 10, 9, 10, 9, 10, 9, 10)); // empty
				List<NamedVector> list = new ArrayList<>();
				for (NamedVector i : iterable) {
					list.add(i);
				}

				assertEquals(0, list.size());
			}
			
		} {
		// test of last tree, it should contain {185, -8924, 171387, 9383, 333}//12 ; {2356, 7823, 8924, -12, 110}//13
			List<PRTree<NamedVector>> trees = result.lookup(5);
			assertEquals(1, trees.size());
			PRTree<NamedVector> tree = trees.get(0);
			assertEquals(2, tree.getNumberOfLeaves()); // 3 vectors by partitions, last one with less
			assertEquals(1, tree.getHeight()); // branch factor = 2
			
			{
				Iterable<NamedVector> iterable = tree.find(new SimpleMBR(100, 3000, -9000, 9000, -89389, 913932892, -6000, 10000, 110, 333)); // 12 and 13
				List<NamedVector> list = new ArrayList<>();
				for (NamedVector i : iterable) {
					list.add(i);
				}
				
				assertTrue(list.contains(new NamedVector("12", new int[]{185, -8924, 17187, 9383, 333})));
				assertTrue(list.contains(new NamedVector("13", new int[]{2356, 7823, 8924, -12, 110})));
				assertEquals(2, list.size());
			} {
				Iterable<NamedVector> iterable = tree.find(new SimpleMBR(184, 186, -8925, -8923, -89389, 913932892, 9000, 10000, 333, 333)); // 12 only
				List<NamedVector> list = new ArrayList<>();
				for (NamedVector i : iterable) {
					list.add(i);
				}
				
				assertTrue(list.contains(new NamedVector("12", new int[]{185, -8924, 17187, 9383, 333})));
				assertEquals(1, list.size());
			}
		} { // test of the whole tree
            List<NamedVector> nn = index.nearestNeighbors(3, new NamedVector("7", new int[]{0, 0, 0, 0, 0}));
            assertEquals(3, nn.size());

            assertTrue(nn.contains(new NamedVector("7", rTreesVectors[7])));
            assertTrue(nn.contains(new NamedVector("2", rTreesVectors[2])));
            assertTrue(nn.contains(new NamedVector("4", rTreesVectors[4])));

            nn = index.nearestNeighbors(5, new NamedVector("7", new int[]{0, 0, 0, 0, 0}));
            assertEquals(5, nn.size());

            assertTrue(nn.contains(new NamedVector("7", rTreesVectors[7])));
            assertTrue(nn.contains(new NamedVector("2", rTreesVectors[2])));
            assertTrue(nn.contains(new NamedVector("4", rTreesVectors[4])));
            assertTrue(nn.contains(new NamedVector("0", rTreesVectors[0])));
            assertTrue(nn.contains(new NamedVector("8", rTreesVectors[8])));

            boolean success = false;
            try {
                nn = index.nearestNeighbors(0, new NamedVector("7", new int[]{0, 0, 0, 0, 0}));
            } catch(IllegalArgumentException e) {
                    success = true;
            } finally {
                assertTrue(success);
            }

            nn = index.nearestNeighbors(13, new NamedVector("0", new int[]{3, 3, 3, 3, 3}));
            assertEquals(13, nn.size());

            assertTrue(nn.contains(new NamedVector(13 + "", rTreesVectors[13])));
            for (int i=0 ; i < 12 ; i++) {
                assertTrue(nn.contains(new NamedVector(i + "", rTreesVectors[i])));
            }
        }
	}
}
