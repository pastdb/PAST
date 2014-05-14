package past.commandLine;

import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.util.Vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class DNApplication {

	private static final Pattern SPACE = Pattern.compile(" ");

    public static void DNAimport () {

    }

    public static void DNAtransformation() {

    }

    public static void DNAsimilarity() {
        //
    }

    public static void test(JavaSparkContext sc) {
    	JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
    	JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1,2,3,4,5,6));

    	//list string
    	List<Integer> toto = rdd2.collect();
    	System.out.println("nombre d'element dans la collect: " + toto.size());
    	System.out.println("premier element dans le rdd: " + rdd2.first());
    	
		for(Integer s:toto) {
			System.out.println(s);
		}

		//list vector
		int size = 3;
		Integer tmp[] = new Integer[3];
		List<Vector> tutu = new ArrayList<Vector>();

		for(int i=0; i<toto.size()-3; i++) {
			List<Integer> pupu = toto.subList(i,i+3);
			pupu.toArray(tmp);

			double tmp2[] = new double[tmp.length];
			for( int j=0; j<tmp.length; j++) {
				tmp2[j] = (double)tmp[j];
			}

		tutu.add(new Vector(tmp2));	
		}
		

		for(Vector v: tutu) {
			System.out.println("vector : " + v);
		}

		System.out.println(".......--------------------");
		
		System.out.println("nombre d'element dans la list vector: " + tutu.size());

		double sup[] = {2,3,4};
		Vector super2 = new Vector(sup);

		for(Vector v: tutu) {
			System.out.println("distance between : " + v + " and " + super2 + " : " + super2.dist(v));
		}

		JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		});

		JavaPairRDD<String, Integer> ones = words.map(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});


		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?,?> tuple : output) {
			System.out.println(tuple._1 + ": " + tuple._2);
		}
    }

}