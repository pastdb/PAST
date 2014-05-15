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
				if(variable.get(key) == null) {
					variable.remove(key);
					continue;
				}
				System.out.println("   key : " + key + " - value : " + variable.get(key));
			}
		}
	}

	/**
	 * DELECT one variable in memory
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
	 * RENAME one variable in memory
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
	 * START spark
	 * user input example: STARTSPARK
	 */
	public static void startSpark() {
		if(sc != null) {
			System.out.println("  spark has already start");
		}
		else {
			sc = new JavaSparkContext("local", "PAST");
			System.out.println("  spark has started");
		}
	}

	/**
	 * STOP spark
	 * user input example: STOPSPARK
	 */
	public static void stopSpark() {
		if(sc == null) {
			System.out.println("  spark has already stop");
		}
		else {
			sc.stop();
			sc = null;
			System.out.println("  spark has stoped");
		}
	}

	/*
	 * EXIT framework
	 */
	public static boolean exit() {
		if(sc != null) {
			sc.stop();
			sc = null;
			System.out.println("  spark has stoped");
		}
		return false;
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

	/*
	 * RESTART database
	 */
	public static void restartDB() {
		if(db == null) {
			System.out.println("  no database open");
		}
		else {
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
				db = new Database(nameDB, hadoopFS, config);
				System.out.println("  database name " + nameDB + ": restart");
			} 
			catch(Exception e) {}	
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
		else if(!db.hasTimeseries(userInput[0])) {
			System.out.println("  no found timeserie");
		}
		else {
			
			String nameTS = userInput[0];
				
			try {
				File dir = new File(".");
				File tsFile;
				tsFile = new File(dir.getCanonicalPath() + File.separator + nameDB + File.separator + "timeseries" + File.separator + nameTS);
				System.out.println(dir.getCanonicalPath() + File.separator + nameDB + File.separator + "timeseries" + File.separator + nameTS);	

				delete(tsFile);
				restartDB();
				System.out.println("   " + nameTS + " is deleted!");
    			
			}
			catch(Exception f) {}
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
	private static int createTS(String userInput[]) {
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
			//System.out.println("  TimeSerie has been created in database name " + nameDB);

			// save in variable
			String v_name = (size == 3) ? userInput[2] : generateNameVariable();  

			Option<Timeseries> tmp = db.getTimeseries(nameTS);
			Timeseries ts = tmp.get();

			variable.put(v_name, ts);
			return 0;
			//System.out.println("  TimeSerie has been save in variable name " + v_name);
			//System.out.println("  with Schema: [ <time, int32> ][ <data, int32 ]");

		}
		else if(!variable.containsKey(nameSchema)) {
			System.out.println("  schema not found");
			System.out.println("  (to create schema tape: CREATE_SCHEMA");
		}
		else {

			Schema schema = (Schema)variable.get(nameSchema);
			db.createTimeseries(nameTS, schema);
			//System.out.println("  TimeSerie has been created in database name " + nameDB);

			// save in variable
			String v_name = (size == 4) ? userInput[3] : generateNameVariable();  

			Option<Timeseries> tmp = db.getTimeseries(nameTS);
			Timeseries ts = tmp.get();

			variable.put(v_name, ts);
			return 0;
			//System.out.println("  TimeSerie has been save in variable name " + v_name);
		}
		return 1;
	}

	/**
	 * CREATE timeSerie to simplify the user
	 * --> original create and insert is now private
	 *
	 * user input example: CREATE nameTS FROM files [ WITH Schema] [: nameVariable] 
	 *
	 * @param array of userinput parameter
	 */
	public static void createTS2DB(String userInput[]) {

		final String FROM = "FROM";
		final String WITH = "WITH";
		final String DELIM = ":";

		int size = userInput.length;
		String nameTS = null;
		String nameSchema = null; 
		String nameFile[] = null;
		String nameVariable = null;
		String v_name = null;
		String tmp = null;

		boolean invalid_input = false;

		int pos_from = -1;
		int pos_with = -1;
		int pos_delim = -1;
		int numberofFiles = -1;

		/* initalisation de value */

		//first element always nameTS
		//last element if : is nameVariable
		for(int i=1; i<size-1; i++) {
			tmp = userInput[i].toUpperCase();
			switch(tmp) {
				case FROM: {
					if(pos_from != -1) invalid_input = true;
					pos_from = i; 
					nameTS = userInput[i-1]; 	
					break;
				}
				case WITH: {
					if(pos_with != -1) invalid_input = true;
					pos_with = i; 	
					nameSchema = userInput[i+1]; 
					break;
				}
				case DELIM: {
					if(pos_delim != -1) invalid_input = true;
					pos_delim = i; 
					nameVariable = userInput[i+1]; 
					break;
				}
				default: break;
			}
		}
		
		/* start to create */

		if(db == null) {
			System.out.println("  no database open");
		}
		else if(invalid_input) {
			System.out.println("  name of many input not allow");	
			System.out.println("  input must be : CREATE nameTS FROM files [ WITH Schema ] [ : nameVariable ]");
		}
		else if(size < 3 || pos_from != 1) {
			System.out.println("  input must be : CREATE nameTS FROM files [ WITH Schema ] [ : nameVariable ]");
		}
		else if(sc == null) {
			System.out.println("  Spark is not start. To start spark, enter: sparkStart");
		}
		else {
			try {
				// our input CREATE nameTS FROM files [ WITH Schema] [: nameVariable] 
				if(pos_delim != -1) {
					v_name = nameVariable;
				}
				else {
					v_name = generateNameVariable(); 
				}

				// CREATE nameTS [Schema]
				String inputCreateTS[] = null;
				if(pos_with != -1) {
					inputCreateTS = new String[4];
					inputCreateTS[0] = nameTS; 
					inputCreateTS[1] = nameSchema;
					inputCreateTS[2] = ":";
					inputCreateTS[3] = v_name;
				} 
				else {
					inputCreateTS = new String[3];
					inputCreateTS[0] = nameTS; 
					inputCreateTS[1] = ":";
					inputCreateTS[2] = v_name;
				}

				// INSERT file TO nameTS
				String inputInsert[] = null;
				if(pos_with != -1 ){
					numberofFiles = pos_with - pos_from - 1;
					inputInsert = new String[numberofFiles + 2];
				}
				else if(pos_delim != -1) {
					numberofFiles = pos_delim - pos_from - 1;
					inputInsert = new String[numberofFiles + 2];

				}
				else {
					numberofFiles = size - pos_from - 1;
					inputInsert = new String[numberofFiles + 2];
				}

				// check validity of number of files
				if(numberofFiles < 1) {
					throw new Exception();
				}
				else {
					for(int i=0; i<numberofFiles; i++) {
						inputInsert[i] = userInput[pos_from + i + 1];
					}
					inputInsert[numberofFiles] = "TO";
					inputInsert[numberofFiles + 1] = v_name;
				}
				
				// create ts in database
				if(createTS(inputCreateTS) != 0) throw new Exception();
				System.out.println("- 1 -");
				// insert input in database
				if(insertDataFromFile(inputInsert) != 0) throw new Exception();
				System.out.println("- 2 -");

				System.out.println("  TimeSerie has been save in variable name " + v_name);

			}
			catch(Exception e) {
				System.out.println("  Create TimeSerie FAIL: check name of files");
				// remove create file 
				try {
					if(variable.keySet().contains(v_name)) variable.remove(v_name);
					File dir = new File(".");
					File tsFile;
					tsFile = new File(dir.getCanonicalPath() + File.separator + nameDB + File.separator + "timeseries" + File.separator + nameTS);
					delete(tsFile);
					restartDB();
				}
				catch(Exception f) {}
			}
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
			System.out.println("  input must be : SHOW_SCHEMA nameVariable");
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
				System.out.println("  Schema of the timeserie: " + v_name);
			}
			catch (Exception e) {
				System.out.println("  the variable is not a Timeserie");
			}
		}
	}

	/*
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
	private static int insertDataFromFile(String userInput[]) {
		int size = userInput.length;
		String nameTS = null;
		String nameFile[] = null;
		int numberFiles = -1;
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
			for(int i=0; i<size-2; i++) {
				// find the TO
				if(userInput[i].toUpperCase().compareTo("TO") == 0) {
					numberFiles = i;
					oneFile = false;

					// name of TS
					nameTS = userInput[numberFiles + 1];
					// save each file in varable
					nameFile = new String[numberFiles];
					for(int j=0; j<numberFiles; j++) {
						nameFile[j] = userInput[j];
					}

					break;
				}
			}
		}
		else {
			//save nothing (numberFile will be negative so it will end)
		}
		

		//helper to not change code more
		int m = numberFiles - 1;

		if(numberFiles < 0) {
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
							throw new Exception();
						}
						else {
							for(int i=0; i<sizeSchema; i++) {
								extractData[i].$plus$eq(new Integer(Integer.parseInt(tmp[i])));
							}
						}
					}

					ListBuffer<Tuple2<String, List<Integer>>> values = new ListBuffer<Tuple2<String, List<Integer>>> ();
					for(int i=1; i<sizeSchema; i++) {
						values.$plus$eq(new Tuple2<String, List<Integer>>(column[i], extractData[i].toList()));
					}

					//insert the content in the databaseIn
					ts.insert(sc.sc(), extractData[0].toList(), values.toList());
					
					//save in the variable name
					//String v_name = (size == 5) ? userInput[4] : generateNameVariable(); 
					//variable.put(v_name, ts);
					//System.out.println("  data to the timeserie has been implemented and saved in variable name " + v_name);

					return 0;
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
				System.out.println("   load many files not implmented yet, use one file with schema [time values]");

			}

		}
		return -1;
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
					//JavaRDD<Integer> colum = new JavaRDD(ts.rangeQuery<Integer>(sc.sc(), columnName, ClassTag$.MODULE$.Int()));
					JavaRDD<Integer> colum =  new JavaRDD(ts.rangeQueryI32(sc.sc(), columnName), ClassManifestFactory$.MODULE$.Int());

					//save in the variable name
					String v_name = (size == 5) ? userInput[4] : generateNameVariable(); 
					variable.put(v_name, colum);
					System.out.println("  data to the timeserie has been implemented and saved in variable name " + v_name);	
				}
				catch (Exception e) {
					System.out.println("   retrieve content of a colum fail, bad name column ?");
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
				JavaRDD<Integer> colum = null;
				try {
					// take column of the timeserie
					colum =  new JavaRDD(ts.rangeQueryI32(sc.sc(), columnName), ClassManifestFactory$.MODULE$.Int());				
				}
				catch (Exception e) {
					System.out.println("   access content of a colum fail");
				}
				
				if(colum != null) {

					int max = colum.reduce(new org.apache.spark.api.java.function.Function2<Integer, Integer, Integer>() {
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
				int min = rdd.reduce(new org.apache.spark.api.java.function.Function2<Integer, Integer, Integer>() {
					public Integer call(Integer a, Integer b) {
						return (a<b) ? a : b;
					}
				});
				System.out.println("  min Value = " + min);
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
				JavaRDD<Integer> colum = null;
				try {
					// take column of the timeserie
					colum =  new JavaRDD(ts.rangeQueryI32(sc.sc(), columnName), ClassManifestFactory$.MODULE$.Int());				
				}
				catch (Exception e) {
					System.out.println("   access content of a colum fail");
				}
				
				if(colum != null) {

					int min = colum.reduce(new org.apache.spark.api.java.function.Function2<Integer, Integer, Integer>() {
						public Integer call(Integer a, Integer b) {
							return (a<b) ? a : b;
						}
					});
					System.out.println("  min Value = " + min);
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
	 * user input example: COMPRESSION nameTS (launch demon parameter config)
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
			System.out.println("  input must be : COMPRESSION nameTS WITH [regression, APCA] ");
			System.out.println("  with more parameter : COMPRESSION nameTS (it will launch demon parameter config) ");
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
			switch(parameter.toUpperCase()) {
				case "APCA": break;
				case "REGRESSION": break;
				default: System.out.println("  bad parameter, user: [regression or APCA]");
			}
		}
		else {
			// default parameter
			String compression_type = "regression";
			String feature_type = "polynomial";
			int max_MAE = 20;
			int min_seg_length = 2;
			int degree = 2;
			int partition = 0;
			int aPCA_seg_cont = 10000;

			System.out.println("  -- config parameter compression -- ");
			Scanner sc = new Scanner(System.in);
			int choice = 0;
			int input = -1;

			// compression type
			try {
				System.out.println("  choose compression type [0]: ");				
				System.out.println("   [0]: regression");
				System.out.println("   [1]: APCA");	
				choice = sc.nextInt();				
			} 
			catch (Exception e) {
				choice = 0;
			}
			switch(choice) {
				case 0: compression_type = "regression";
				case 1: compression_type = "APCA";
				default: compression_type = "regression";
			}

			// feature type
			try {
				System.out.println("  choose feature type [0]: ");				
				System.out.println("   [0]: polynomial");
				System.out.println("   [1]: time");	
				choice = sc.nextInt();				
			} 
			catch (Exception e) {
				choice = 0;
			}
			switch(choice) {
				case 0: feature_type = "polynomial";
				case 1: feature_type = "time";
				default: feature_type = "polynomial";
			}

			// fmax MAE
			try {
				System.out.println("  enter max MAE [20]: ");				
				input = sc.nextInt();	
				choice = 1;			
			} 
			catch (Exception e) {
				choice = -1;
			}
			switch(choice) {
				case 1: max_MAE = input;
				default: max_MAE = 20;
			}

			// min_seg_length
			try {
				System.out.println("  enter min seg length [2]: ");				
				input = sc.nextInt();	
				choice = 1;			
			} 
			catch (Exception e) {
				choice = -1;
			}
			switch(choice) {
				case 1: min_seg_length = input;
				default: min_seg_length = 2;
			}

			// degree
			try {
				System.out.println("  enter degree [2]: ");				
				input = sc.nextInt();	
				choice = 1;			
			} 
			catch (Exception e) {
				choice = -1;
			}
			switch(choice) {
				case 1: degree = input;
				default: degree = 2;
			}

			// partitions
			try {
				System.out.println("  enter partitions [0]: ");				
				input = sc.nextInt();	
				choice = 1;			
			} 
			catch (Exception e) {
				choice = -1;
			}
			switch(choice) {
				case 1: partition = input;
				default: partition = 0;
			}

			// aPCA_seg_cont
			try {
				System.out.println("  enter APCA seg cont [10000]: ");				
				input = sc.nextInt();	
				choice = 1;			
			} 
			catch (Exception e) {
				choice = -1;
			}
			switch(choice) {
				case 1: aPCA_seg_cont = input;
				default: aPCA_seg_cont = 10000;
			}



		}
	}

	/* ************************************
	 * indexing 
	 *************************************/

	public static void indexing() {
		
	}

	/* ************************************
	 * clustering 
	 *************************************/

	/* ************************************
	 * Application 
	 *************************************/

	/**
	 * DNA_SIMILARITY BETWEEN dna1 AND dna2 (brutforce)
	 * user input example: DNA_SIMILARITY BETWEEN dna1 AND dna2
	 */
	public static void dnApplication(String userInput[]) {
		int size = userInput.length;

		if(size != 4) {
			System.out.println("  input must be : DNA_SIMILARITY BETWEEN dna1 AND dna2");
		}
		else if(userInput[0].toUpperCase().compareTo("BETWEEN") != 0) {
			System.out.println("  you forget to put 'BETWEEN'");	
		}
		else if(userInput[2].toUpperCase().compareTo("AND") != 0) {
			System.out.println("  you forget to put 'AND'");	
		}
		else if(!variable.keySet().contains(userInput[1])) {
			System.out.println("  no found timeserie (RDD). To get a RDD use : SELECT colum FROM nameTS");
		}
		else if(!variable.keySet().contains(userInput[3])) {
			System.out.println("  no found timeserie (RDD). To get a RDD use : SELECT colum FROM nameTS");
		}
		else {
			String name_dna1 = userInput[1];
			String name_dna2 = userInput[3];

			Object ob1 = variable.get(name_dna1);
			Object ob2 = variable.get(name_dna2);

			JavaRDD<String> dna1 = null;
			JavaRDD<String> dna2 = null;

			try {
				dna1 = (JavaRDD<String>)ob1;
				dna2 = (JavaRDD<String>)ob2;
			}
			catch(Exception e) {
				System.out.println("  the variable is not a RDD");
			}

			if(dna1 != null && dna2 != null) {

				try {
					startSpark();
					DNApplication.DNAsimilarity(sc, dna1, dna2);	
				}
				catch(Exception e){
					System.out.println("  FAIL in DNA similarity");
				}
			}	
		}
	}


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

    /*
     * delect a directory or a file
     */
    private static void delete(File file)throws Exception{
 		// it s a directory
    	if(file.isDirectory()){
     		// directory empty
    		if(file.list().length==0) file.delete(); 	 
    		// directory non empty
    		else{
        	   	String filesList[] = file.list();
 
 				// recursive delect file or folder
        	   	for (String childFile : filesList) {
        	      	File file2Delete = new File(file, childFile);
        	    	delete(file2Delete);
        	   	}
 
        	   	// directory is now emty
        	   	if(file.list().length==0) file.delete();  
    		}
 
    	}
    	// it a file and not a directory
    	else file.delete();
    }

    /*
     * normal int comparator
     */
    static class NormalIntComparator implements Comparator<Integer>, Serializable {
    	@Override
    	public int compare(Integer a, Integer b) {
      		if (a > b) return 1;
      		else if (a < b) return -1;
      		else return 0;
    	}
  	};

  	/*
  	 * reverse int comparator
  	 */
    static class ReverseIntComparator implements Comparator<Integer>, Serializable {
    	@Override
    	public int compare(Integer a, Integer b) {
      		if (a > b) return -1;
      		else if (a < b) return 1;
      		else return 0;
    	}
  	};
}
