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
import java.util.Scanner;

import scala.*;

import past.storage.*;


/**
 * ExecuteCommand Class execute the user command. 
 * This class link the userInterface and other useful class to allow 
 * user work on timeserie.
 *
 */
public class ExecuteCommand {

	/* database information */
	private static Database db = null;
	private static String nameDB = null;

	/* variable save by the user */
	private static Map<String, Object > variable = new HashMap<String, Object >();
	private static int varIndice = 0;
	private static final String varName = "var";


	/* ************************************
	 * standard commands
	 *************************************/
	public static void showVar() {
		if(variable.isEmpty()) {
			System.out.println("  no variable save");
		}
		else {
			for (String key: variable.keySet()) {
				System.out.println("key : " + key + " - value : " + variable.get(key));
			}
		}
	}

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
			System.out.println("  no database open");
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
		int size = userInput.length;

		if(db == null) {
			System.out.println("  no database open");
		}
		else if(size != 1) {
			System.out.println("  input must be : DROP 'name of timeSerie' ");
		} 
		else {
			String nameTS = userInput[0];
			if(!db.hasTimeseries(nameTS)) {
				System.out.println("  no found timeserie");
			}
			else {
				// TODO
				System.out.println("  right now, no DROP of timeSerie is possible ");
			}
		}
	}

	/*
	 * EXIST timeSerie
	 */
	public static void existTS(String userInput[]) {
		int size = userInput.length;

		if(db == null) {
			System.out.println("  no database open");
		}
		else if(size != 1) {
			System.out.println("  input must be : EXIST 'name of timeSerie' ");
		} 
		else {
			String nameTS = userInput[0];
			boolean exist = db.hasTimeseries(nameTS);
			if(exist) {
				System.out.println("  yes");
			}
			else {
				System.out.println("  no");
			}
		}
	}

	/*
	 * GET timeSerie
	 */
	public static void getTS(String userInput[]) {
		int size = userInput.length;
		String nameTS = null;

		if(size > 0) {
			nameTS = userInput[0];
		}

		if(db == null) {
			System.out.println("  no database open");
		}
		else if(size < 1 || size > 3 || size == 2) {
			System.out.println("  input must be : GET 'name of timeSerie' : [name] ");
		} 
		else if(size == 3 && userInput[1].compareTo(":") != 0) {
			System.out.println("  you forget to put ':'");
		}
		else if(size == 3 && variable.keySet().contains(userInput[2])) {
			System.out.println("  variable name already exist");
		}
		else if(!db.hasTimeseries(nameTS)) {
			System.out.println("  no found timeserie");
		}
		else {
			String v_name = (size == 3) ? userInput[2] : generateNameVariable();  
	
			Option<Timeseries> tmp = db.getTimeseries(nameTS);
			Timeseries ts = tmp.get();
			variable.put(v_name, ts);
			System.out.println("  TimeSerie has been save in variable name " + v_name);
		}
	}


	/*
	 * CREATE timeSerie
	 */
	public static void createTS(String userInput[]) {
		int size = userInput.length;
		String nameTS = null;
		String nameSchema = null; 

		if(size > 1) {
			nameTS = userInput[0];
			nameSchema = userInput[1];
		}

		if(db == null) {
			System.out.println("  no database open");
		}
		else if(size < 2 || size > 4 || size == 3) {
			System.out.println("  input must be : CREATE 'name of timeSerie' 'Schema' [: name]");
		} 
		else if(size == 4 && userInput[2].compareTo(":") != 0) {
			System.out.println("  you forget to put ':'");
		}
		else if(size == 4 && variable.keySet().contains(userInput[3])) {
			System.out.println("  variable name already exist");
		}
		else if(db.hasTimeseries(nameTS)) {
			System.out.println("  name of timeserie already exist");
		}
		else if(!variable.containsKey(nameSchema)) {
			System.out.println("  schema not found");
			System.out.println("  (to create schema tape: CREATE_SCHEMA");
		}
		else {

			Schema schema = (Schema)variable.get(nameSchema);
			db.createTimeseries(nameTS, schema);
			System.out.println("  TimeSerie has been created in database name " + nameDB);
			
			// save in variable
			String v_name = (size == 4) ? userInput[3] : generateNameVariable();  
			
			Option<Timeseries> tmp = db.getTimeseries(nameTS);
			Timeseries ts = tmp.get();
			variable.put(v_name, ts);
			System.out.println("  TimeSerie has been save in variable name " + v_name);
		}
	}

	/* ************************************
	 * Time Series function
	 *************************************/

	/*
	 * CREATE_SCHEMA for the timeSerie
	 */
	public static void createSchema() {

//		try {
//			System.out.println("  CREATE schema:");
//
//			Scanner sc = new Scanner(System.in);
//			System.out.println("    enter the number of field:");
//			int n = sc.nextInt();
//			
//			Field schemaField[] = new Field[n];
//			DBType type[] = {DBInt32, DBInt64, DBFloat32, DBFloat64};
//			String nameField = null;
//			
//			
//			for(int i=0; i<n; i++) {
//				System.out.println("    enter the name of the " + n+1 + " field :");
//				nameField = sc.nextLine().trim();
//				
//				System.out.println("    select the value type : [0]");
//				System.out.println("     [0] int32");
//				System.out.println("     [1] int64");
//				System.out.println("     [2] float32");
//				System.out.println("     [3] float64");
//				//System.out.println("     [4] string");
//				n = sc.nextInt();
//				
//			}
//		
//		}
//		catch (Exception e) {
//			System.out.println("    create schema fail: invalid input");
//		}
		//Schema s = new Schema(new Tuple2<String, DBType.DBType<?>>("temps", DBInt32));
		//Schema s = new Schema(Field("temps", DBInt32));
	}

	/*
	 * SHOW_SCHEMA of the timeSerie
	 */
	public static void showSchema(String userInput[]) {
		int size = userInput.length;
		String nameTS = null;

		if(size > 0) {
			nameTS = userInput[0];
		}

		if(size != 1) {
			System.out.println("  input must be : GET 'name of timeSerie'");
		} 
		else if(!variable.keySet().contains(nameTS)) {
			System.out.println("  Timeserie not found");
		}
		else {
			
			Object ob = variable.get(nameTS);
			try {
				Schema schema = ( (Timeseries)ob ).schema();
				System.out.println("  Schema of the timeserie: ");
				System.out.println(schema.toString());
			}
			catch (Exception e) {
				System.out.println("  the variable is not a Timeserie");
			}
		}

	}

	/*
	 * GET_SCHEMA of the timeSerie
	 */
	public static void getSchema(String userInput[]) {
		int size = userInput.length;
		String nameTS = null;
		
		if(size > 0) {
			nameTS = userInput[0];
		}
		
		if(size < 1 || size > 3 || size == 2) {
			System.out.println("  input must be : GET 'name of timeSerie' [: name]");
		} 
		else if(size == 3 && userInput[1].compareTo(":") != 0) {
			System.out.println("  you forget to put ':'");
		}
		else if(size == 3 && variable.keySet().contains(userInput[2])) {
			System.out.println("  variable name already exist");
		}
		else if(!variable.keySet().contains(nameTS)) {
			System.out.println("  Timeserie not found");
		}
		else {
			Object ob = variable.get(nameTS);
			try {
				Schema schema = ( (Timeseries)ob ).schema();
				String v_name = (size == 3) ? userInput[2] : generateNameVariable();
				variable.put(v_name, schema);
				System.out.println("  Schema of the timeserie: ");
			}
			catch (Exception e) {
				System.out.println("  the variable is not a Timeserie");
			}
		}
	}

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

	/* ************************************
	 * helper function
	 *************************************/
	private static String generateNameVariable() {
		varIndice += 1;
		return varName + varIndice;
	}


}
