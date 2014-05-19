package past.index.iSAX;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import past.storage.*;
import past.storage.DBType;
import scala.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import com.typesafe.config.Config; 
import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.util.*;
import java.io.*;
import org.apache.spark.SparkContext;
import past.Timeseries;
import past.Transformations;
import java.awt.Point;

import java.util.Hashtable;
import past.index.iSAX.iSAX_Index;
public class iSAX_Indexmain{

private static Database db = null;
	private static String nameDB = null;
	public static JavaSparkContext sc ;
	public static Timeseries ts;
	
	private static void print_ts(Hashtable<Integer,java.lang.Object> ts_data){
		for (Map.Entry<Integer, java.lang.Object> entry : ts_data.entrySet()) {
		    System.out.println(entry.getKey()+" : "+entry.getValue());
			}
	}
	
public static String readFile( String file ) throws IOException {
    BufferedReader reader = new BufferedReader( new FileReader (file));
    String         line = null;
    StringBuilder  stringBuilder = new StringBuilder();
    String         ls = System.getProperty("line.separator");

    while( ( line = reader.readLine() ) != null ) {
        stringBuilder.append( line );
    }

    return stringBuilder.toString();
}
public static ArrayList<Timeseries> generateDNATS(String dnaType, String path)throws IOException {



	String dnaString=readFile(path);
		System.out.println("Starting conversion......."+dnaString.length()/1024+"::"+dnaString.length());
	dnaString.trim();
		ArrayList<Timeseries> ts_array=new ArrayList<Timeseries>(Math.round(dnaString.length()/1024));
	int window_sep=0;
	int part=0;
	double prev=0;
	
	Hashtable<java.lang.String,Hashtable<Integer,java.lang.Object>> ts_hash= new Hashtable<java.lang.String,Hashtable<Integer,java.lang.Object>>();
	Hashtable<Integer,java.lang.Object> ts_data = new Hashtable<Integer,java.lang.Object>();
	for (int i=0;i<dnaString.length();i++){
	
		char dna = dnaString.charAt(i);
			
		 if (dna == 'A') {
		 		prev+=(double)2.0;
			   ts_data.put(Integer.valueOf(window_sep) ,java.lang.Double.valueOf(prev));
			   
			   
			 }
			 if (dna == 'G') {
			 prev+=(double)1.0d;
			   ts_data.put(Integer.valueOf(window_sep) ,java.lang.Double.valueOf(prev));

			 }
			 if (dna == 'C') {
			 prev+=(double)-1.0;
			  ts_data.put(Integer.valueOf(window_sep) ,java.lang.Double.valueOf(prev));

			 }
			 if (dna == 'T') {
			 prev+=(double)-2.0;
			  ts_data.put(Integer.valueOf(window_sep),java.lang.Double.valueOf(prev));
			 }
			 
		window_sep++;
		
		if(window_sep==1024){
			if(ts_data.isEmpty()){
				System.out.println("Empty set");
				System.exit(1);
			}
			Hashtable<java.lang.String,Hashtable<Integer,java.lang.Object>> final_Ts=new Hashtable<java.lang.String,Hashtable<Integer,java.lang.Object>>();
			final_Ts.put(dnaType+part,ts_data);
	
			Timeseries ts=new Timeseries(dnaType+part,final_Ts,DBType.DBInt32$.MODULE$);
			ts.timeStart=0;
			ts.timeEnd=1024;
			ts.setName(dnaType+part);
			ts_array.add(part,ts);
			ts_data = new Hashtable<Integer,java.lang.Object>();
			window_sep=0;
			part++;
			prev=0;
		}
			
	}
	if(ts_array.isEmpty()){
				System.out.println("Empty TS list (in creation function)");
				System.exit(1);
			}
	return ts_array;
	
}

 public static Timeseries generateRandomTS(int len,String name2) throws java.io.IOException,java.net.URISyntaxException {
 
 //ArrayList<Integer> times =new ArrayList<Integer>(Arrays.asList(1,2,3,4,5,6,7,8,9));
  //  ArrayList<java.lang.Double> data = new ArrayList<java.lang.Double>(Arrays.asList(1.1d, 1.2d, 1.3d, 1.4d, 1.5d, 1.6d, 1.7d, 1.8d, 1.9d));
   Random r= new Random();
String ts_names[]=new String[100];
	FileSystem filesystem = FileSystem.get(new URI("file:///tmp"), new Configuration());
	String name = "pastdb_timeseries_"+java.lang.System.nanoTime();
	
	Hashtable<Integer,java.lang.Object> ts_data = new Hashtable<Integer,java.lang.Object>();
	for(int i=0;i<len;i++){
		if(i%2==0)
		ts_data.put(i+i*2,i*r.nextDouble());
		else
		ts_data.put(i+i*2,i/r.nextDouble());
	}
	Hashtable<java.lang.String,Hashtable<Integer,java.lang.Object>> ts_hash= new Hashtable<java.lang.String,Hashtable<Integer,java.lang.Object>>();
	ts_hash.put(name2,ts_data);
	Timeseries t=new Timeseries(name2,ts_hash,DBType.DBInt32$.MODULE$);
	
	try{
  /* fileSystem (hadoop) */
				FileSystem hadoopFS = FileSystem.get(new URI("file:///tmp"), new Configuration());

				/* path for config (hadoop) */
				String currentDir = System.getProperty("user.dir");
				Path path = new Path(currentDir);
				Map mapConfig = new HashMap();
				mapConfig.put("path", path.toString());

				/* config (hadoop) */
				Config config = ConfigFactory.parseMap(mapConfig);
				
				System.out.println("  - opening DB -");
				System.out.println("   Current dir using System:" + System.getProperty("user.dir")); 

			
		} 
			catch(java.net.URISyntaxException e) {
				System.out.println(e);
			}
   
return t;
   // return ts_names;

  }
public static boolean create_index(int dimensions, int cardinality){

	Hashtable<Integer,Point> SAX = Transformations.symbolicAggregateApproximation(ts, "sax_index", 0, 50, dimensions, cardinality);
	for (Map.Entry<Integer, java.awt.Point> entry : SAX.entrySet()) {
		
		    System.out.println(entry.getKey()+" : "+entry.getValue().toString());
			}
	return true;
}

public static void main(String args[]){
	String logFile = "/home/jasmina/spark-0.9.1-bin-hadoop1/README.md"; // Should be some file on your system
    sc= new JavaSparkContext("local", "Simple App",
      "/home/jasmina/spark-0.9.1-bin-hadoop1", new String[]{});
    JavaRDD<String> logData = sc.textFile(logFile).cache();
	double indexS=0;
	double indexE=0;
    try{
    	int size=Integer.parseInt(args[0]);
    	int dimensions=Integer.parseInt(args[1]);
    	int cardinality=Integer.parseInt(args[2]);
	 // Timeseries t1= generateRandomTS(size,"T1");
	//  print_ts(t1.getTimeseries().get("T1"));
	//  Timeseries t2= generateRandomTS(size+20,"T2");
	    String currentDir = System.getProperty("user.dir");
		Path path = new Path(currentDir);
	    indexS=System.currentTimeMillis();
	    	iSAX_Index index=new iSAX_Index();
		 ArrayList<Timeseries> human_ts = iSAX_Indexmain.generateDNATS("human",currentDir+"/DNA1.txt");
		 ArrayList<Timeseries> chimp_ts=iSAX_Indexmain.generateDNATS("hypo",currentDir+"/DNA2.txt");
		indexE=System.currentTimeMillis();
		double TS_generationT=indexE-indexS;
		 if(human_ts.isEmpty()){
		 	System.out.println("EMPTY HUMAN DNA");
		 	System.exit(1);
		 }
		 if(chimp_ts.isEmpty()){
		 	System.out.println("EMPTY CHIMP DNA");
		 	System.exit(1);
		 }
		 
		 //****** TO BE IMPLEMENTED USING RDD
		 int k=0;
		 indexS=System.currentTimeMillis();
		 for(int i=0;i<human_ts.size();i++){
		 	human_ts.get(i).setPath(new Path(currentDir,human_ts.get(i).getName())); //** NO NEED WHEN USING RDD
			if(human_ts.get(i).getTimeseries().isEmpty()){
				System.out.println("Empty timeseries, can not build index");
				System.exit(1);
			}
		 	index.set_dataset(human_ts.get(i),sc); //*** start: INSTEAD OF THIS , IF IT IS THE FIRST TS CALL configure_iSAX_index_RDD
		 	if(k==0){
		 		if(index.build_index()) //*** THIS WILL BE HANDLED IN THE configure_iSAX....
		 		
		 		System.out.println("Index built successfuly");
		 		k=-1;	 		
		 	
		 	} //*** end: 
		 	else{
		 		index.insert_raw(human_ts.get(i)); //***INSTEAD OF THIS CALL insert_raw_RDD(JavaRDD<Integer> timestamp,JavaRDD<Double> values, String ts_name, Path path);
		 	}

		 }
		 indexE=System.currentTimeMillis();
		 System.out.println("All humanDNA samples added to the index");
		double timeS=System.currentTimeMillis();
		System.out.println("Trying to find approximate matches to TS1");
		Timeseries minTS=chimp_ts.get(0);
		double min_app_dist=-1;
		double min_exact_dist=-1;
		double tmp_app=-1;
		double tmp_exact=-1; //*** THIS IS SEARCHING FOR THE MATCH
		for(int i=0;i<chimp_ts.size();i++){
		chimp_ts.get(i).setPath(path); //**NO NEED 
			tmp_app=iSAXQuery.ApproximateSearch(index,chimp_ts.get(i),0,1024); //CALL  ApproximateSearchRDD(iSAX_Index index, JavaRDD<Integer> timestamp,JavaRDD<Double> values,String ts_name, int start, int stop);
			if(min_app_dist<0 || min_app_dist>tmp_app){
				min_app_dist=tmp_app;
				minTS=chimp_ts.get(i);
			}
			
		}		
		System.out.println("Closet distance=" + min_app_dist);
	//	System.out.println("Closest DNA:: "+ iSAX_Indexmain.TS2DNA(minTS));
		
		min_exact_dist=5000;
		for(Timeseries ts : chimp_ts){
			tmp_app=iSAXQuery.ExactSearch(index,ts,ts.getName(),0,1024); //CALL ExactSearchRDD(iSAX_Index index, JavaRDD<Integer> timestamp,JavaRDD<Double> values, String ts_name, int start, int stop);
			
			if(min_exact_dist<0 || min_exact_dist>tmp_app){
				System.out.println("Changing min dist");
				min_exact_dist=tmp_app;
				minTS=ts;
			}
			
		}
		
		System.out.println("Closet exact distance=" + min_exact_dist+"::"+minTS.getName()); 
		
	//	System.out.println("Closest DNA:: "+ iSAX_Indexmain.TS2DNA(minTS));
		double timeE=System.currentTimeMillis();
		System.out.println("TS_GENERATION_TIME::CLOCK::"+ TS_generationT/1000);
		System.out.println("INDEX_GENERATION_TIME::CLOCK::"+ (indexE-indexS)/1000);
		System.out.println("EXECUTION_TIME::CLOCK::"+(timeE-timeS)/(double)1000);
	   }catch (java.io.IOException e){
	   	System.out.println (e);
	   }
	   catch (java.net.URISyntaxException ue){
	   	System.out.println(ue);
	   }
  }


}
