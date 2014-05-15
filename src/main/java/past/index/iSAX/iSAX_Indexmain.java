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
      "/home/jasmina/spark-0.9.1-bin-hadoop1", new String[]{"target/simple-project-1.0.jar"});
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    try{
    	int size=Integer.parseInt(args[0]);
    	int dimensions=Integer.parseInt(args[1]);
    	int cardinality=Integer.parseInt(args[2]);
	  Timeseries t1= generateRandomTS(size,"T1");
	  print_ts(t1.getTimeseries().get("T1"));
	  Timeseries t2= generateRandomTS(size+20,"T2");
	    String currentDir = System.getProperty("user.dir");
		Path path = new Path(currentDir);
	    
	    	iSAX_Index index=new iSAX_Index();
		 
		t1.setPath(path);
	
		t2.setPath(path);
		
		index.set_dataset(t1,sc);
		if(index.build_index())
			System.out.println("Index built successfully");
		t1.setName("T2");
		
	//	t1.setName("T3");
	//	index.set_dataset(t1,sc);
	//	if(index.insert_raw(t1)){
	//		System.out.println("T2 added to index");
	//	}
		System.out.println("Trying to find approximate matches to TS1");		
		//HashMap<String, Path> index_file = 
		System.out.println("Closet distance="+iSAXQuery.ApproximateSearch(index,t2,index.getStart(),index.getStop()));
		System.out.println("Exact distance="+iSAXQuery.ExactSearch(index,t2,t2.getName(),index.getStart(),index.getStop()));
	//	if(null!=index_file)
		//	for (Map.Entry<String, Path> entry2 : index_file.entrySet()) {
		//		System.out.println("TS::"+entry2.getKey().toString()+"::PATH::"+entry2.getValue().toString());
		//	}		
	//	else{
		//	System.out.println("No matching file found");
		//}
	  //  create_index(dimensions,cardinality);
	   }catch (java.io.IOException e){
	   	System.out.println (e);
	   }
	   catch (java.net.URISyntaxException ue){
	   	System.out.println(ue);
	   }
  }


}
