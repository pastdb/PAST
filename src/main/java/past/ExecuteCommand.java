package past;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

import com.typesafe.config.Config; 
import com.typesafe.config.ConfigFactory;

import java.nio.file.FileSystems;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import past.storage.*;


/**
 * ExecuteCommand Class execute the user command. 
 * This class link the userInterface and other useful class to allow 
 * user work on timeserie.
 *
 */
public class ExecuteCommand {


	private static Database db = null;
	private static String nameDB = null;
	/* ************************************
	 * database function
	 *************************************/ 

	/*
	 * OPEN or CREATE timeSerie using Timeseries.scala
	 */
	public static void openDB(String userInput[]) {
		int size = userInput.length;
		
		if(db != null) {
			System.out.println("  one database already open");
		}
		else if(size < 1 || size > 2) {
			//System.out.println("  input must be : OPEN name filesystem [conf]");
			System.out.println("  input must be : OPEN name ");
		} 
		else {
			String name = userInput[0];
			//String filesystem = userInput[1];
			//String conf = (size == 3) ? userInput[2] : null;
			
			try {
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
				/* create or open database */
				db = new Database(name, hadoopFS, config);
				nameDB = name;
				System.out.println("   " + db);
			} 
			catch(Exception e) {
				System.out.println(e);
			}	
		}
	}

	/*
	 * CLOSE database
	 */
	public static void closeDB() {
		if(db == null) {
			System.out.println("  no database open");
		}
		else {
			System.out.println("  database name " + nameDB + ": close");
			db = null;
			nameDB = null;
		}
	}
	
	/*
	 * SHOW list of timeSeries 
	 */
	public static void showTS() {
		if(db == null) {
			System.out.println("  no database is open");
		}
		else {
			String TSlist[] = db.getTimeseries();
			System.out.println("  LIST of TIMESERIES :");
			
			if(TSlist.length == 0 ) {
				System.out.println("   no TimeSeries in database"); 
			}
			else {
				for(String s: TSlist) {
					System.out.println("    - " + s);
				}
			}
		}
	}
	
	/*
	 * DROP timeSerie
	 */
	public static void dropTS(String userInput[]) {
		
	}
	
	/*
	 * EXIST timeSerie
	 */
	public static void existTS(String userInput[]) {
		
	}
	
	/*
	 * GET timeSerie
	 */
	public static void getTS(String userInput[]) {
		
	}
	
	/*
	 * CREATE timeSerie
	 */
	public static void createTS(String userInput[]) {
		
	}
	
	
	/* ************************************
	 * Time Series function
	 *************************************/

	/* ************************************
	 * Transformations function
	 *************************************/

	/* ************************************
	 * Compression 
	 *************************************/

	/* ************************************
	 * indexing 
	 *************************************/

	/* ************************************
	 * clustering 
	 *************************************/

	/* ************************************
	 * Forecasting 
	 *************************************/


}
