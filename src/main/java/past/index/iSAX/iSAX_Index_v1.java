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

public class iSAX_Index_v1{

private static Database db = null;
	private static String nameDB = null;
	public static JavaSparkContext sc ;
	//Timeseries$ generateRandomTS(int len) {
 public static void generateRandomTS(int len) throws java.io.IOException,java.net.URISyntaxException {
 Timeseries ts;
 ArrayList<Integer> times =new ArrayList<Integer>(Arrays.asList(1,2,3,4,5,6,7,8,9));
    ArrayList<java.lang.Float> data = new ArrayList<java.lang.Float>(Arrays.asList(1.1f, 1.2f, 1.3f, 1.4f, 1.5f, 1.6f, 1.7f, 1.8f, 1.9f));
   
String ts_names[]=new String[100];
	FileSystem filesystem = FileSystem.get(new URI("file:///tmp"), new Configuration());
	String name = "pastdb_timeseries_"+java.lang.System.nanoTime();
	//Schema schema=new SchemaConstructor("ts",DBType.DBInt32$.MODULE$).get();//,("data",DBType.DBFloat32$.MODULE$)).get();
	try{
  /* fileSystem (hadoop) */
				FileSystem hadoopFS = FileSystem.get(new URI("file:///tmp"), new Configuration());

				/* path for config (hadoop) */
				String currentDir = System.getProperty("user.dir");
				Path path = new Path(currentDir);
				Map mapConfig = new HashMap();
				mapConfig.put("path", path.toString());

				/* config (hadoop) */
				/*Config config = ConfigFactory.parseMap(mapConfig);

				System.out.println("  - opening DB -");
				System.out.println("   Current dir using System:" + System.getProperty("user.dir")); */
				/* create or open database */
				/*db = new Database(name, hadoopFS, config);
				db.createTimeseries("T",schema);
				ts_names=db.getTimeseries();
				ts=db.getTimeseriesAsJava("T");
				HashMap<java.lang.String,ArrayList<java.lang.Float>> map= new HashMap<java.lang.String,ArrayList<java.lang.Float>>();
				map.put("data",data);*/
			//	SparkContext spark= new SparkContext("local", "Simple App","/home/jasmina/spark-0.9.1-bin-hadoop1", new String[]{"target/simple-project-1.0.jar"});
			//	ts.insert(spark, times, Arrays.asList(map));
			/*	nameDB = name;
				System.out.println("   " + db); */

		} 
			catch(java.net.URISyntaxException e) {
				System.out.println(e);
			}
    for (int x = 0; x < len; x++) {

      int r = (int) (Math.random() * 100) % 10;

     // ts.add(new TPoint(r, x));

    }

   // return ts_names;

  }
public boolean create_index(JavaRDD<java.lang.Double> ts){


	
	return true;
}

public static void main(String args[]){
	String logFile = "/home/jasmina/spark-0.9.1-bin-hadoop1/README.md"; // Should be some file on your system
    sc= new JavaSparkContext("local", "Simple App",
      "/home/jasmina/spark-0.9.1-bin-hadoop1", new String[]{"target/simple-project-1.0.jar"});
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter(new Function<String, java.lang.Boolean>() {
      public java.lang.Boolean call(String s) { return s.contains("a"); }
    }).count();

    long numBs = logData.filter(new Function<String, java.lang.Boolean>() {
      public java.lang.Boolean call(String s) { return s.contains("b"); }
    }).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    try{
	    generateRandomTS(100);
	   }catch (java.io.IOException e){
	   	System.out.println (e);
	   }
	   catch (java.net.URISyntaxException ue){
	   	System.out.println(ue);
	   }
  }


}