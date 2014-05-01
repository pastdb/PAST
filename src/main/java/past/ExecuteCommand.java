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
				System.out.println(db);
				/* create or open database */
				db = new Database(name, hadoopFS, config);
				System.out.println("   " + db);
			} 
			catch(Exception e) {
				System.out.println(e);
			}	
		}
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
