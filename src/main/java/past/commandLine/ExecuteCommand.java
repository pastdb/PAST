package past.commandLine;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.JavaRDD.*;
import org.apache.spark.api.java.function.*;

import com.typesafe.config.Config; 
import com.typesafe.config.ConfigFactory;

import java.nio.file.FileSystems;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Comparator;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import scala.collection.convert.WrapAsJava$;
import scala.reflect.ClassTag$;
import scala.reflect.ClassManifestFactory$;

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

	/**
	 * show every existing variable, the type and the location memory
	 * user input example: SHOW 
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

	/**
	 * delect one variable in memory
	 * user input example: DEL nameVariable
	 * 
	 * @param array of userinput parameter
	 */
	public static void delVar(String userInput[]) {
		int size = userInput.length;

		if(size < 1) {
			System.out.println("  input must be : DEL varName [varName2] [varNameX]");
		} 
		else if(variable.isEmpty()) {
			System.out.println("  no variable save");
		}
		else {
			for(String s: userInput) {
				if(variable.keySet().contains(s)) {
					variable.remove(s);
					System.out.println("   variable " + s + " is removed");
				}
				else {
					System.out.println("   variable " + s + " not found");	
				}
			}
		}
	}

	/**
	 * rename one variable in memory
	 * user input exxample: RENAME varName newVarName
	 * 
	 * @param array of userinput parameter
	 */
	public static void renameVar(String userInput[]) {
		int size = userInput.length;

		if(size != 2) {
			System.out.println("  input must be : RENAME varName new_varName");
		}
		else if(variable.isEmpty()) {
			System.out.println("  no variable save");
		}
		else if(!variable.keySet().contains(userInput[0])) {
			System.out.println("   variable not found");
		}
		else if(variable.keySet().contains(userInput[1])) {
			System.out.println("   new variable name already exist");
		}
		else {
			String varName = userInput[0];
			String newName = userInput[1];
			Object tmp = variable.get(varName);
			variable.remove(varName);
			variable.put(newName, tmp);
			System.out.println("   rename done");
		}
	}

	/**
	 * start spark
	 * user input example: STARTSPARK
	 */
	public static void startSpark() {
		if(sc != null) {
			System.out.println("  spark has already start");
		}
		else {
			sc = new JavaSparkContext("local", "PAST");
		}
	}

	/**
	 * stop spark
	 * user input example: STOPSPARK
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

	/**
	 * OPEN or CREATE timeSerie using Timeseries.scala
	 * user input example: OPEN nameDB
	 *
	 * @param array of userinput parameter
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

	/**
	 * CLOSE database
	 * user input example: CLOSE
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

	/**
	 * SHOW list of timeSeries 
	 * user input example: SHOW
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

	/**
	 * DROP timeSerie
	 * user input example: DROP nameTS
	 *
	 * @param array of userinput parameter
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

	/**
	 * EXIST timeSerie
	 * user input example: EXIST nameTS
	 *
	 * @param array of userinput parameter
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

	/**
	 * GET timeSerie
	 * user input example: GET nameTS [: nameVariable] 
	 *
	 * @param array of userinput parameter
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
			System.out.println("  input must be : GET 'name of timeSerie' [: nameVariable] ");
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


	/**
	 * CREATE timeSerie
	 * user input example: CREATE nameTS [Schema] [: nameVariable] 
	 *
	 * @param array of userinput parameter
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
			System.out.println("  input must be : CREATE 'name of timeSerie' [Schema] [: nameVariable]");
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

	/**
	 * CREATE_SCHEMA for the timeSerie
	 * user input example: CREATE_SCHEMA [: nameVariable] 
	 *
	 * @param array of userinput parameter
	 */
	public static void createSchema(String userInput[]) {
		int size = userInput.length;

		if(size < 0 || size > 2 || size == 1) {
			System.out.println("  input must be : CREATE_SCHEMA [: nameVariable]");
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

				SchemaConstructor schemaCons = new SchemaConstructor("times", DBType.DBInt32$.MODULE$);
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

	/**
	 * SHOW_SCHEMA of the timeSerie
	 * user input example: SHOW_SCHEMA nameVariable 
	 *
	 * @param array of userinput parameter
	 */
	public static void showSchema(String userInput[]) {
		int size = userInput.length;
		String nameTS = null;

		if(size == 1) {
			nameTS = userInput[0];
		}

		if(size != 1) {
			System.out.println("  input must be : GET_SCHEMA nameVariable");
		} 
		else if(!variable.keySet().contains(nameTS)) {
			System.out.println("  Timeserie or schema not found");
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

	/**
	 * GET_SCHEMA of the timeSerie
	 * user input example: GET_SCHEMA FROM nameTS [: nameVariable] 
	 *
	 * @param array of userinput parameter
	 */
	public static void getSchema(String userInput[]) {
		int size = userInput.length;
		String nameTS = null;

		if(size > 1) {
			nameTS = userInput[1];
		}

		if(size < 2 || size > 4 || size == 3) {
			System.out.println("  input must be : GET_SCHEMA FROM 'name of timeSerie' [: nameVariable]");
		} 
		else if(userInput[0].toUpperCase().compareTo("FROM") != 0) {
			System.out.println("  you forget to put 'FROM'");
		}
		else if(size == 4 && userInput[2].compareTo(":") != 0) {
			System.out.println("  you forget to put ':'");
		}
		else if(size == 4 && variable.keySet().contains(userInput[3])) {
			System.out.println("  variable name already exist");
		}
		else if(!variable.keySet().contains(nameTS)) {
			System.out.println("  Timeserie not found");
		}
		else {
			Object ob = variable.get(nameTS);
			try {
				Schema schema = ( (Timeseries)ob ).schema();
				String v_name = (size == 4) ? userInput[3] : generateNameVariable();
				variable.put(v_name, schema);
				System.out.println("  Schema of the timeserie: ");
			}
			catch (Exception e) {
				System.out.println("  the variable is not a Timeserie");
			}
		}
	}

	/**
	 * INSERT data at a certain faile
	 * user input example: INSERT file TO nameTS [: nameVariable] 
	 * file input struct [times values values ...]
	 *
	 * user input example: INSERT file1 file2 ... TO nameTS [: nameVariable] 
	 * file1 = timestamp of timeserie (integer)
	 * file2... = values of one colum of timeserie (integer)
	 *
	 * @param array of userinput parameter
	 */
	public static void insertDataFromFile(String userInput[]) {
		int size = userInput.length;
		String nameTS = null;
		String nameFile[] = null;
		int numberFiles = -1;
		java.lang.Boolean stop = false;
		java.lang.Boolean oneFile = true;

		// have only one file 
		if(size > 2 && userInput[1].toUpperCase().compareTo("TO") == 0) {
			nameTS = userInput[2];
			nameFile = new String[1];
			nameFile[0] = userInput[0];
			numberFiles = 1;
		}
		// find number of file use
		else if(size > 2) {
			for(int i=0; i<size; i++) {
				if(userInput[i].toUpperCase().compareTo("TO") == 0) {
					numberFiles = i;
					oneFile = false;
					break;
				}
			}
			if(size >= numberFiles + 2) {
				nameTS = userInput[numberFiles + 1];
				nameFile = new String[numberFiles];
				for(int i=0; i<numberFiles; i++) {
					nameFile[i] = userInput[i];
				}
			}
		}
		else {
			stop = true;
		}

		//helper to not change code more
		int m = numberFiles - 1;

		if(stop || numberFiles < 0) {
			System.out.println("  input must be : INSERT files TO timeserie [: nameVariable]");
		}
		else if(size < 3+m || size > 5+m || size == 4+m) {
			System.out.println("  input must be : INSERT file TO timeserie [: nameVariable]");
		}
		else if(size == 5+m && userInput[3+m].compareTo(":") != 0) {
			System.out.println("  you forget to put ':'");
		}
		else if(size == 5+m && variable.keySet().contains(userInput[4+m])) {
			System.out.println("  variable name already exist");
		}
		else if(userInput[1+m].toUpperCase().compareTo("TO") != 0)  {
			System.out.println("  you forget to put 'TO'");	
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

			/***************************
			 *
			 * upload with one files
			 *
			 ***************************/

			// read file and load element and value of the timeserie
			if(ts != null && oneFile) {
				File dir = new File(".");
				File tsFile;
				FileInputStream reader = null;
				BufferedReader data = null;
				
				try {
					tsFile = new File(dir.getCanonicalPath() + File.separator + nameFile[0]);
					reader = new FileInputStream(tsFile);
					data = new BufferedReader(new InputStreamReader(reader));
					
					//information a propos du schema
					Schema schema = ts.schema();
					int sizeSchema = schema.fields().size();
					java.lang.Iterable<String> column_schema = scala2javaIterable(schema.fields().keys());
					String column[] = iterable2array(column_schema);

					String line = null;
					String tmp[] = null;
					@SuppressWarnings("unchecked")
					ListBuffer<Integer> extractData[] = new ListBuffer[sizeSchema]; 
					// extractData[0] -> times
					// extractData[1] -> values
					for(int i=0; i<sizeSchema; i++) extractData[i] = new ListBuffer<Integer>();
				
			
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
								extractData[i].$plus$eq(new Integer(Integer.parseInt(tmp[i])));
							}
						}
					}

					ListBuffer<Tuple2<String, ListBuffer<Integer>>> values = new ListBuffer<Tuple2<String, ListBuffer<Integer>>> ();
					for(int i=1; i<sizeSchema; i++) {
						values.$plus$eq(new Tuple2<String, ListBuffer<Integer>>(column[i], extractData[i]));
					}

					//insert the content in the database
					// TODO
					//ts.insert(sc.sc(), extractData[0].toList(), values.toList());
					
					//save in the variable name
					String v_name = (size == 5) ? userInput[4] : generateNameVariable(); 
					variable.put(v_name, ts);
					System.out.println("  data to the timeserie has been implemented and saved in variable name " + v_name);
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

			/***************************
			 *
			 * upload with many files
			 *
			 ***************************/
			
			// read file and load element and value of the timeserie
			if(ts != null && !oneFile) {
				File dir = new File(".");
				File tsFile;
				FileInputStream reader = null;
				BufferedReader data = null;
				
				// TODO
			}

		}
	}


	/*
	 * SELECT_RANGE of timeserie from timeStart to timeEnd
	 */


	/**
	 * SELECT a colum from timeserie
	 * user input example: SELECT colum FROM nameTS [: nameVariable] 
	 *
	 * @param array of userinput parameter
	 */
	public static void selectColumn(String userInput[]) {
		int size = userInput.length;
		String nameTS = null;
		String columnName = null;

		if(size >= 3) {
			nameTS = userInput[2];
			columnName = userInput[0];
		}

		if(size < 3 || size > 5 || size == 4) {
			System.out.println("  input must be : SELECT colum FROM timeserie [: nameVariable]");
		}
		else if(size == 5 && userInput[3].compareTo(":") != 0) {
			System.out.println("  you forget to put ':'");
		}
		else if(size == 5 && variable.keySet().contains(userInput[4])) {
			System.out.println("  variable name already exist");
		}
		else if(userInput[1].toUpperCase().compareTo("FROM") != 0) {
			System.out.println("  you forget to put 'FROM'");	
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
				try {

					// TODO
					//JavaRDD<Integer> colum = new JavaRDD(ts.rangeQuery<Integer>(sc.sc(), columnName, ClassTag$.MODULE$.Int()));
					JavaRDD<Integer> colum =  null;//new JavaRDD(ts.rangeQueryI32(sc.sc(), columnName, ClassTag$.MODULE$.Int()), ClassManifest$.MODULE$.Int());

					//save in the variable name
					String v_name = (size == 5) ? userInput[4] : generateNameVariable(); 
					variable.put(v_name, colum);
					System.out.println("  data to the timeserie has been implemented and saved in variable name " + v_name);	
				}
				catch (Exception e) {
					System.out.println("   retrieve content of a colum fail");
				}
				
			}
		}
	}

	/**
	 * MAX_VALUE of a timeserie
	 * user input example: MAX_VALUE colum FROM nameTS 
	 * user input example: MAX_VALUE varName(type RDD) 
	 *
	 * @param array of userinput parameter
	 */
	public static void maxValue(String userInput[]) {
		int size = userInput.length;
		String nameTS = null;
		String columnName = null;

		// if size=1 --> check only in variable for a RDD
		// if size=3 --> take colum of a TS to have a RDD
		if(size == 1) {
			nameTS = userInput[0];
		}
		else if(size == 3) {
			nameTS = userInput[2];
			columnName = userInput[0];
		}

		if(size < 1 || size > 3 || size == 2) {
			System.out.println("  input must be : MAX_VALUE colum FROM timeserie or MAX_VALUE variable");
		}
		else if(size == 3 && userInput[1].toUpperCase().compareTo("FROM") != 0) {
			System.out.println("  you forget to put 'FROM'");	
		}
		else if(size == 3 && !variable.keySet().contains(nameTS)){
			System.out.println("  Timeserie not found");
		}
		else if(size == 1 && !variable.keySet().contains(nameTS)) {
			System.out.println("  Timeserie not found");
		}
		else if(sc == null) {
			System.out.println("  Spark is not start. To start spark, enter: sparkStart");
		}
		else if(size == 1) {
			Object ob = variable.get(nameTS);
			JavaRDD<Integer> rdd = null;

			try{
				rdd = (JavaRDD<Integer>)ob;
			}
			catch (Exception e) {
				System.out.println("  the variable is not a RDD");
			}


			if(rdd != null) {
				//int max = Integer.MIN_VALUE;
				int max = rdd.reduce(new org.apache.spark.api.java.function.Function2<Integer, Integer, Integer>() {
					public Integer call(Integer a, Integer b) {
						return (a>b) ? a : b;
					}
				});
				System.out.println("  max Value = " + max);
			}

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
				JavaRDD<Integer> rdd = null;
				try {

					// TODO






					// TODO
					rdd = null;//new JavaRDD(ts.rangeQuery(sc.sc(), columnName));
					
				}
				catch (Exception e) {
					System.out.println("   retrieve content of a colum fail");
				}
				
				if(rdd != null) {

					int max = rdd.reduce(new org.apache.spark.api.java.function.Function2<Integer, Integer, Integer>() {
						public Integer call(Integer a, Integer b) {
							return (a>b) ? a : b;
						}
					});
					System.out.println("  max Value = " + max);
				}
				
			}
		}
	}



	/**
	 * MIN_VALUE of a timeserie
	 * user input example: MIN_VALUE colum FROM nameTS 
	 * user input example: MIN_VALUE varName(type RDD) 
	 *
	 * @param array of userinput parameter
	 */
	public static void minValue(String userInput[]) {
		int size = userInput.length;
		String nameTS = null;
		String columnName = null;

		// if size=1 --> check only in variable for a RDD
		// if size=3 --> take colum of a TS to have a RDD
		if(size == 1) {
			nameTS = userInput[0];
		}
		else if(size == 3) {
			nameTS = userInput[2];
			columnName = userInput[0];
		}

		if(size < 1 || size > 3 || size == 2) {
			System.out.println("  input must be : MAX_VALUE colum FROM timeserie or MAX_VALUE variable");
		}
		else if(size == 3 && userInput[1].toUpperCase().compareTo("FROM") != 0) {
			System.out.println("  you forget to put 'FROM'");	
		}
		else if(size == 3 && !variable.keySet().contains(nameTS)){
			System.out.println("  Timeserie not found");
		}
		else if(size == 1 && !variable.keySet().contains(nameTS)) {
			System.out.println("  Timeserie not found");
		}
		else if(sc == null) {
			System.out.println("  Spark is not start. To start spark, enter: sparkStart");
		}
		else if(size == 1) {
			Object ob = variable.get(nameTS);
			JavaRDD<Integer> rdd = null;

			try{
				rdd = (JavaRDD<Integer>)ob;
			}
			catch (Exception e) {
				System.out.println("  the variable is not a RDD");
			}

			if(rdd != null) {
				int max = rdd.reduce(new org.apache.spark.api.java.function.Function2<Integer, Integer, Integer>() {
					public Integer call(Integer a, Integer b) {
						return (a<b) ? a : b;
					}
				});
				System.out.println("  min Value = " + max);
			}

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
				JavaRDD<Integer> rdd = null;
				try {

					// TODO





					// TODO
					rdd = null;//new JavaRDD(ts.rangeQuery(sc.sc(), columnName));
					
				}
				catch (Exception e) {
					System.out.println("   retrieve content of a colum fail");
				}
				
				if(rdd != null) {
					int max = rdd.reduce(new org.apache.spark.api.java.function.Function2<Integer, Integer, Integer>() {
						public Integer call(Integer a, Integer b) {
							return (a<b) ? a : b;
						}
					});
					System.out.println("  max Value = " + max);
				}
				
			}
		}
	}



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

	/**
	 * COMPRESSION of a timeserie
	 * user input example: COMPRESSION nameTS WITH [regression, APCA , demon]
	 * user input example: COMPRESSION nameTS (default parameter)
	 *
	 * @param array of userinput parameter
	 */
	public static void compression(String userInput[]) {
		int size = userInput.length;
		String nameTS = null;
		String parameter = null;

		
		if(size > 1) {
			nameTS = userInput[0];
		}
		else if(size == 3) {
			nameTS = userInput[0];
			parameter = userInput[2];
		}

		if(size < 1 || size > 3 || size == 2) {
			System.out.println("  input must be : COMPRESSION nameTS [WITH regression, APCA] ");
		}
		else if(size == 3 && userInput[1].toUpperCase().compareTo("WITH") != 0) {
			System.out.println("  you forget to put 'WITH'");	
		}
		else if(size == 3 && !variable.keySet().contains(nameTS)){
			System.out.println("  Timeserie not found");
		}
		else if(size == 1 && !variable.keySet().contains(nameTS)) {
			System.out.println("  Timeserie not found");
		}
		else if(sc == null) {
			System.out.println("  Spark is not start. To start spark, enter: sparkStart");
		}
		else if(size == 3) {
			// do compression with default parameter
		}
		else {
			// do a demon to choose parameter
		}
	}

	/* ************************************
	 * indexing 
	 *************************************/



	/* ************************************
	 * clustering 
	 *************************************/

	/* ************************************
	 * Application 
	 *************************************/

	/* ************************************
	 * helper function
	 *************************************/

	/*
	 * generate name variable
	 */
	private static String generateNameVariable() {
		String newname = null;
		do {
			varIndice += 1;	
			newname = varName + varIndice;
		}
		while(variable.keySet().contains(newname));
		
		return newname;
	}

	/*
	 * convert java.lang.Iterable<String> to String[]
	 */
	private static String[] iterable2array(java.lang.Iterable<String> iterable) {
		ArrayList<String> array = new ArrayList<String>();
		for(String s: iterable) {
			array.add(s);
		}
		return array.toArray(new String[array.size()]);
	}

	/*
	 * convert scala.collection.Seq<String> to java.util.List<String> 
	 */
	private static java.util.List<String> seq2list(scala.collection.Seq<String> seq) {
        return WrapAsJava$.MODULE$.seqAsJavaList(seq);
    }

    /*
     * convert scala.collection.Iterable<String> to java.lang.Iterable<String> 
     */
    private static java.lang.Iterable<String> scala2javaIterable(scala.collection.Iterable<String> scala_iterable) {
		return WrapAsJava$.MODULE$.asJavaIterable(scala_iterable);
					
    }

    static class NormalIntComparator implements Comparator<Integer>, Serializable {
    	@Override
    	public int compare(Integer a, Integer b) {
      		if (a > b) return 1;
      		else if (a < b) return -1;
      		else return 0;
    	}
  	};

    static class ReverseIntComparator implements Comparator<Integer>, Serializable {
    	@Override
    	public int compare(Integer a, Integer b) {
      		if (a > b) return -1;
      		else if (a < b) return 1;
      		else return 0;
    	}
  	};
}
