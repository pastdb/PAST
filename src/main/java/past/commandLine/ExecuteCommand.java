package past.commandLine;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;

import com.typesafe.config.Config; 
import com.typesafe.config.ConfigFactory;

import java.nio.file.FileSystems;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.ArrayList;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import scala.*;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.mutable.ListBuffer;

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

	/* spark context */
 	private static JavaSparkContext sc = null;


	/* ************************************
	 * standard commands
	 *************************************/
	/*
	 * show every existing variable, the type and the location memory
	 */
	public static void showVar() {
		if(variable.isEmpty()) {
			System.out.println("  no variable save");
		}
		else {
			for (String key: variable.keySet()) {
				System.out.println("   key : " + key + " - value : " + variable.get(key));
			}
		}
	}

	/*
	 *	start spark
	 */
	public static void startSpark() {
		if(sc != null) {
			System.out.println("  spark has already start");
		}
		else {
			sc = new JavaSparkContext("local", "PAST");
		}
	}

	/*
	 * stop spark
	 */
	public static void stopSpark() {
		if(sc == null) {
			System.out.println("  spark has already stop");
		}
		else {
			sc.stop();
			sc = null;
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
			System.out.println("  input must be : GET 'name of timeSerie' [: name] ");
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

		if(size > 0) nameTS = userInput[0];
		if(size > 1) nameSchema = userInput[1];

		if(db == null) {
			System.out.println("  no database open");
		}
		else if(size < 1 || size > 4) {
			System.out.println("  input must be : CREATE 'name of timeSerie' [Schema] [: name]");
		} 
		else if(size == 3 && userInput[1].compareTo(":") != 0) {
			System.out.println("  you forget to put ':'");
		}
		else if(size == 3 && variable.keySet().contains(userInput[2])) {
			System.out.println("  variable name already exist");
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
		else if(size == 1 || size == 3) {
			SchemaConstructor schemaCons = new SchemaConstructor("time", DBType.DBInt32$.MODULE$);
			schemaCons.addField("data", DBType.DBInt32$.MODULE$);
			Schema schema = schemaCons.get();
			System.out.println(" schema : " + schema);
			db.createTimeseries(nameTS, schema);
			System.out.println("  TimeSerie has been created in database name " + nameDB);

			// save in variable
			String v_name = (size == 3) ? userInput[2] : generateNameVariable();  

			Option<Timeseries> tmp = db.getTimeseries(nameTS);
			Timeseries ts = tmp.get();
			variable.put(v_name, ts);
			System.out.println("  TimeSerie has been save in variable name " + v_name);
			System.out.println("  with Schema: [ <time, int32> ][ <data, int32 ]");

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
	public static void createSchema(String userInput[]) {
		int size = userInput.length;

		if(size < 0 || size > 2 || size == 1) {
			System.out.println("  input must be : CREATE_SCHEMA [: name]");
		}
		else if(size == 2 && userInput[0].compareTo(":") != 0) {
			System.out.println("  you forget to put ':'");
		}
		else if(size == 2 && variable.keySet().contains(userInput[1])) {
			System.out.println("  variable name already exist");
		}
		else {
			try {
				System.out.println("  CREATE schema:");

				Scanner sc = new Scanner(System.in);
				System.out.println("    enter the number of field:");
				int n = sc.nextInt();
				sc.nextLine();

				SchemaConstructor schemaCons = new SchemaConstructor("timestamps", DBType.DBInt32$.MODULE$);
				DBType.DBType<?> type[] = {
					DBType.DBInt32$.MODULE$, 
					DBType.DBInt64$.MODULE$, 
					DBType.DBFloat32$.MODULE$, 
					DBType.DBFloat64$.MODULE$
				};

				String nameField = null;

				for(int i=0; i<n; i++) {
					System.out.println("    enter the name of the " + (i+1) + " field :");
					nameField = sc.nextLine().trim();

					do {
						System.out.println("    select the value type : [0]");
						System.out.println("     [0] int32");
						System.out.println("     [1] int64");
						System.out.println("     [2] float32");
						System.out.println("     [3] float64");
						//System.out.println("     [4] string");
						n = sc.nextInt();
						sc.nextLine();
						
					} while (n < 0 || n > 4);

					schemaCons.addField(nameField, type[n]);
				}
				Schema s = schemaCons.get();

				String v_name = (size == 2) ? userInput[1] : generateNameVariable(); 
				variable.put(v_name, s);
				System.out.println("  Schema has been created and saved in variable name " + v_name);
			}
			catch (Exception e) {
				System.out.println("    create schema fail: invalid input");
			}
		}	
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
			System.out.println("  input must be : GET 'name of timeSerie or schema'");
		} 
		else if(!variable.keySet().contains(nameTS)) {
			//System.out.println("  Timeserie not found");
		}
		else {

			Object ob = variable.get(nameTS);
			boolean error = true;
			String msg_error = "   the variable is not: ";

			try { // timeserie
				Schema schema = ( (Timeseries)ob ).schema();
				System.out.println("  Schema of the timeserie: ");
				System.out.println(schema.toString());
				error = false;
			}
			catch (Exception e) {
				msg_error = msg_error + "a Timeserie ";
			}

			try { // schema
				Schema schema = ( (Schema)ob );
				System.out.println("  Schema of the schema: ");
				System.out.println(schema.toString());
				error = false;
			}
			catch (Exception e) {
				msg_error = msg_error + "or a Schema";
			}

			if(error) System.out.println(msg_error);
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
			System.out.println("  input must be : GET_SCHEMA 'name of timeSerie' [: name]");
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

	/*
	 * INSERT data at a certain faile
	 */
	public static void insertDataFromFile(String userInput[]) {
		int size = userInput.length;
		String nameTS = null;
		String nameFile = null;

		if(size > 2) {
			nameTS = userInput[0];
			nameFile = userInput[1];
		}

		if(size < 2|| size > 4 || size == 3) {
			System.out.println("  input must be : INSERT timeserie file [: name]");
		}
		else if(size == 4 && userInput[2].compareTo(":") != 0) {
			System.out.println("  you forget to put ':'");
		}
		else if(size == 4 && variable.keySet().contains(userInput[3])) {
			System.out.println("  variable name already exist");
		}
		else if(!variable.keySet().contains(nameTS)){
			System.out.println("  Timeserie not found");
		}
		else if(sc == null) {
			System.out.println("  Spark is not start. To start spark, enter: sparkStart");
		}
		else {

			Object ob = variable.get(nameTS);
			Timeseries ts = null;
			try {
				ts = (Timeseries)ob;
			}
			catch(Exception e) {
				System.out.println("  the variable is not a Timeserie");
			}

			if(ts != null) {
				File dir = new File(".");
				File tsFile;
				FileInputStream reader = null;
				BufferedReader data = null;
			try {
				tsFile = new File(dir.getCanonicalPath() + File.separator + nameFile);
				reader = new FileInputStream(tsFile);
					data = new BufferedReader(new InputStreamReader(reader));
					
					int sizeSchema = 2;
					
					String line = null;
					String tmp[] = null;
					@SuppressWarnings("unchecked")
					ArrayList<String> extractData[] = new ArrayList[sizeSchema]; 
					// Length of tab is number of column of the schema. 
					for(int i=0; i<sizeSchema; i++) extractData[i] = new ArrayList<String>();
					
					while ((line = data.readLine()) != null) {
						line = line.replaceAll("\\s+"," ");
						tmp = line.split(" ");
							
						if(line.compareTo("") == 0){}
						else if(tmp.length != sizeSchema) {
							System.out.println("   schema unfit for data: find " + tmp.length + " field and require " + sizeSchema + " field");
							break;
						}
						else {
							for(int i=0; i<sizeSchema; i++) {
								extractData[i].add(tmp[i]);
							}
						}
					}
					//TODO
					//demander eric pour extract nombre et name field schema
					//faire list 
					//ts.insert(sc, extractData[0], scala.List)
					for(int i=0; i<sizeSchema; i++) {
						System.out.println(extractData[i]);
					}
				}
				catch (Exception e) {
					System.out.println("   Reading data in file FAIL");
				}
				finally {
					try {
						if (reader != null) reader.close();
						if (data != null) data.close();
					}
					catch(Exception e) {}
				}

			}
		}
	}


	/*
	 * SELECT_RANGE of timeserie from timeStart to timeEnd
	 */



	/*
	 * MAX_VALUE of a timeserie
	 */



	/*
	 * MIN_VALUE of timeserie
	 */



	/*
	 * MAX_TIMESTAMP of timeserie
	 */



	/*
	 * MIN_TIMESTAMP of timeserie
	 */




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
