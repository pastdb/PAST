package past.commandLine;

import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.util.Vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import java.util.Collections;
import java.util.Comparator;

public class DNApplication {

	private static final Pattern SPACE = Pattern.compile(" ");



	/**
	 * method to do a brut force similairty DNA
	 */
	public static void startSimDNA(JavaSparkContext sc_, JavaRDD<Integer> dnaWanted_, JavaRDD<Integer> dna_) {
        JavaRDD<Integer> rdd1 = dna_;		
    	JavaRDD<Integer> rdd2 = dnaWanted_;	

    	
    	List<String> DNAwanted = ts2DNA(rdd2.collect());
     	List<String> data = ts2DNA(rdd1.collect());

        System.out.println("-----------------1----------------------");
        System.out.println("lengh of the DNA wanted : " + DNAwanted.size());
        System.out.println("lengh of the DNA : " + data.size());
        System.out.println("----------------------------------------");

     	DNAsimilarity(sc_, DNAwanted, data);
    }

    /**
     * convert string to ts (int)
     */
    public static List<String> ts2DNA(List<Integer> dna) {
    	int size = dna.size();
    	String ts[] = new String[size];

    	for(int i=0; i<size; i++) {
    		if(i==0) ts[i] = char2intDNA(dna.get(i));
    		else {
    			ts[i] = char2intDNA(dna.get(i) - dna.get(i-1) );
    		}
    	}	
    	return Arrays.asList(ts);
    }

    /**
     * convert ts(int) to string
     */
    public static List<Integer> DNA2ts(List<String> dna) {
    	int size = dna.size();
    	Integer ts[] = new Integer[size];
    	
    	for(int i=0; i<size; i++) {
    		if(i==0) ts[i] = convertingDNA(dna.get(i));
    		else ts[i] = ts[i-1] + convertingDNA(dna.get(i));
    	}	
    	return Arrays.asList(ts);
    }

    /*
     * convert weight of chromozome (string to int)
     */
    public static int convertingDNA (String s) {
    	switch(s.toUpperCase().trim()) {
    		case "A": return 2;
    		case "G": return 1;
    		case "C": return -1;
    		case "T": return -2;
    		default: return 0;
    	}
    }

    /*
     * convert weight of chromozome (int to string)
     */
    private static String char2intDNA (int n) {
    	switch(n) {
    		case 2: return "A";
    		case 1: return "G";
    		case -1: return "C";
    		case -2: return "T";
    		default: return "_";
    	}
    }



    /**
     * method to do a brut force similarity 
     * sliding windows on the time serie in integer or double values
     *
     * @param javaSparkContext
     */
    private static void DNAsimilarity(JavaSparkContext sc_, List<String> dnaWanted_, List<String> dna_) {
        //JavaRDD<String> rdd1 = dna_;		//sc_.parallelize(Arrays.asList( "a", "g", "c", "t", "a"));
    	//JavaRDD<String> rdd2 = dnaWanted_;	//sc_.parallelize(Arrays.asList( "g", "c"));

    	// genere each combinaison of DNA
    	List<String> DNAwanted = dnaWanted_; //rdd2.collect();
     	List<String> data = dna_;//rdd1.collect();
     	int DNAwanted_size = (int)DNAwanted.size();

        System.out.println("-----------------2----------------------");
        System.out.println("lengh of the DNA wanted : " + DNAwanted.size());
        System.out.println("lengh of the DNA : " + data.size());
        System.out.println("----------------------------------------");

     	List<Tuple2<Integer, Vector>> slidingWindows = DNAslideWindows(slideWindows(data, DNAwanted_size));
    	
    	// compaire dna and seqDNA
    	JavaRDD<Tuple2<Integer, Vector>> dna = sc_.parallelize(slidingWindows);
    	final Vector seqDNA = DNA2timeSerie(DNAwanted);

/*   	// check output
		System.out.println("----------------1------------------" );
    	for(Tuple2<Integer, Vector> v: slidingWindows) {
			System.out.println("vector : " + v);
		}
		System.out.println("----------------2------------------" );
		System.out.println(seqDNA );
		System.out.println("----------------3------------------" );
*/

     	// map -->  tuple(index, vector), distance
    	JavaPairRDD<Tuple2<Integer, Vector>, Double> ones = dna.map(new PairFunction<Tuple2<Integer, Vector>, Tuple2<Integer, Vector>, Double>() {
			@Override
			public Tuple2<Tuple2<Integer, Vector>, Double> call(Tuple2<Integer, Vector> v) {
				return new Tuple2<Tuple2<Integer, Vector>, Double>(v, seqDNA.dist(v._2));
			}
		});

   	// check output
/*
    	System.out.println("----------------4------------------" );
    	List<Tuple2<Tuple2<Integer, Vector>, Double>> yop = ones.collect();
		for(Tuple2<Tuple2<Integer, Vector>, Double> s: yop ) {
			System.out.println("content : " + s);
		}
		System.out.println("----------------5------------------" );
*/
     	// reduce
		JavaPairRDD<Tuple2<Integer, Vector>, Double> filter = ones.filter(new Function<Tuple2<Tuple2<Integer, Vector>, Double>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Tuple2<Integer, Vector>, Double> x) {
				return x._2 < 5;
			} 
		});

    	// check output
		System.out.println("----------------6------------------" );
    	List<Tuple2<Tuple2<Integer, Vector>, Double>> yop2 = filter.collect();
		for(Tuple2<Tuple2<Integer, Vector>, Double> s: yop2 ) {
			System.out.println("content : " + s);
		}
		System.out.println("----------------7------------------" );


		// output
		List<Tuple2<Tuple2<Integer, Vector>, Double>> listSimilar = ones.collect();
		int nElement = listSimilar.size();

		Collections.sort( listSimilar, new Comparator< Tuple2<Tuple2<Integer, Vector>, Double> >(){
			@Override
        	public int compare(Tuple2<Tuple2<Integer, Vector>, Double>  a, Tuple2<Tuple2<Integer, Vector>, Double>  b) {
        		if (a._2 > b._2) return 1;
      			else if (a._2 < b._2) return -1;
      			else return 0;
        	}
		});

		System.out.println("\n\n   number of similar dna with epsilon=0.2: " + nElement);
		System.out.println("   wanted DNA: [" + DNA2String(DNAwanted) + "]\n" );

		int top = 3;
		if(nElement > 3) System.out.println("   top 3 sequence : ");
		else if (nElement >= 2) {
			System.out.println("   top 2 sequence : ");
			top = 2;
		}
		else if (nElement >= 1) {
			System.out.println("   top 1 sequence : ");
			top = 1;
		}
		else {
			top = 0;
		}

		Tuple2<Tuple2<Integer, Vector>, Double> topTS = null;
		for(int i=0; i<top; i++) {
			topTS = listSimilar.get(i);
			System.out.println("   - index:" + topTS._1._1 + " similarity:" + topTS._2 + "sequence:");
			System.out.println("     DNA: [" + DNA2String(DNAsubstract(data, topTS._1._1, DNAwanted_size)) + "]\n");
		}
    }

    /*
     * convert dna to a timeseries
     */
    private static Vector DNA2timeSerie(List<String> dna) {
    	int size = dna.size();
    	double ts[] = new double[size];
    	
    	for(int i=0; i<size; i++) {
    		if(i==0) ts[i] = (double)convertingDNA(dna.get(i));
    		else ts[i] = ts[i-1] + (double)convertingDNA(dna.get(i));
    	}	

    	return new Vector(ts);
    }


    /*
     * list all the windows (slide Windows) keeping the position
     */
    private static List<Tuple2<Integer, List<String>>> slideWindows(List<String> data_, int length_) {
    	int size = data_.size();
		List<Tuple2<Integer, List<String>>> windowsList = new ArrayList<Tuple2<Integer, List<String>>>();

		for(int i=0; i<=data_.size()-length_; i++) {
			windowsList.add(new Tuple2<Integer, List<String>>(i, data_.subList(i,i+length_)));	
		}
    	return windowsList;
    }

    /*
     * special convertion of sliding windows dna to timeserie
     */
    private static List<Tuple2<Integer, Vector>> DNAslideWindows(List<Tuple2<Integer, List<String>>> data_) {
    	List<Tuple2<Integer, Vector>> result = new ArrayList<Tuple2<Integer, Vector>>();

    	for(Tuple2<Integer,List<String>> t: data_) {
    		result.add(new Tuple2<Integer, Vector>(t._1, DNA2timeSerie(t._2)));
    	}
    	return result;
    }

    private static List<String> DNAsubstract(List<String> data_, int pos_, int lengthDNA_) {
    	return data_.subList(pos_, pos_+lengthDNA_);
    }

    /*
     * transform a List<Integer> to Vector
     */
    private static Vector intList2vector(List<Integer> data_) {
    	int size = data_.size();
    	double int2doubleValue[] = new double[size];
    	for(int i=0; i<size; i++) {
    		int2doubleValue[i] = (double)data_.get(i);
    	}
    	return new Vector(int2doubleValue);
    }

    /*
     * convert timeserie DNA to string
     */
    private static String DNA2String(List<String> data_) {
     	String dna = "";
     	for(String s: data_) {
     		dna = dna + s.trim();
     	}

     	return dna.toString();
    }

    //-----------  bad implementation of Isax Paper ---------------
    // 				   have to tell other people
    //	because they don't create a timeserie as in the paper but
    //  only converte the dna to integer
    //-------------------------------------------------------------
    

    /**
     * method to let me test spark
     */
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