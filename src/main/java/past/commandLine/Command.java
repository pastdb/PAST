package past.commandLine;

import org.apache.spark.api.java.*;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Command class contain available command that user can use and the help function.
 * - check user command
 * - execute the user command if the command is valid (using ExecuteCommand Class)
 */
public class Command {

	/*  
	 * List of commands available on the console 
	 * 
	 * - adding commands must also to be add in command() function and help() function
	 * - the executing command will be on the class Execute
	 */
	static String commands[] = {

		/* standard commands */
		"QUIT", "EXIT", "HELP", "VAR", "DEL", "RENAME", "STOPSPARK", "STARTSPARK",

		/* database */
		"OPEN", "CLOSE", "RESTART", "SHOW", "DROP", "EXIST", "GET", "CREATE",
		/* Time Serie */

		"CREATE_SCHEMA", "SHOW_SCHEMA", "GET_SCHEMA", /*"INSERT",*/ /*"SELECT_RANGE",*/ 
		"MAX_VALUE", "MIN_VALUE", "PRINT", "SELECT", 

		/* Transformations */  // --> dosn't have any function ...
		//"SQRT TRANSFORM","LOG TRANSFORM", "MEAN", "SHIFT", 
		//"SCALE", "STD DEVIATION", "NORMALIZE", "SEARCH", "MOVING AVERAGE", "DFT",
		
		/* Compression */ // --> dosn't work on the storage ...
		"COMPRESSION", /*"DECOMPRESSION",*/

		/* indexing */ // --> one little error one converting table to rdd
		"CREATE_INDEX", /*"NEIGHBORS",/*

		/* clustering */
		
		/* Forecasting */

		/* Application */
		"DNA_SIMILARITY",
	};

	/* help to use contain */
	static private ArrayList<String> commandsList = new ArrayList<String>(Arrays.asList(commands));
	
	/**
	 * 
	 * @param userCommandLine
	 * @return
	 */
	public static boolean executeCommand(String userInput[]) {
		int size = userInput.length;

		if(userInput[0].length() == 0) { // no input
			//do nothing
		}
		else if(!commandsList.contains(userInput[0].toUpperCase()) ) {
			System.out.println("unknown command");
		}
		else { // not save in variable
			Object return_value = command(userInput);
			if(return_value instanceof Boolean) {
				return (boolean)return_value;
			}
		}
		return true;
	}
	
	/*
	 * execute Past API command
	 */
	private static Object command(String userCommandLine[]) {
		int size = userCommandLine.length;

		switch(userCommandLine[0].trim().toUpperCase()) {

		/*
		 * INFORMATION
		 *
		 * Add executecommand on each case
		 * but if it dosn't yet implemented
		 * comment the command allow to user
		 * on the variable string[] commands
		 *
		 */
		
		/* ************************************
		 * standard commands
		 *************************************/
		case "QUIT": return ExecuteCommand.exit();
		case "EXIT": return ExecuteCommand.exit();
		case "HELP": help( (size > 1) ? userCommandLine[1].trim() : ""); break;
		case "VAR": ExecuteCommand.showVar(); break;
		case "DEL" : ExecuteCommand.delVar(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		case "RENAME": ExecuteCommand.renameVar(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		case "STARTSPARK": ExecuteCommand.startSpark(); break;
		case "STOPSPARK": ExecuteCommand.stopSpark(); break;
		
		/* ************************************
		 * database
		 *************************************/ 
		
		/* open database(name: String, filesystem: FileSystem, conf: Config) */
		case "OPEN" : ExecuteCommand.openDB(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* close database */
		case "CLOSE" : ExecuteCommand.closeDB(); break;
		/* restart database */
		case "RESTART" : ExecuteCommand.restartDB(); break;
		/* show list of TimeSeries */
		case "SHOW" : ExecuteCommand.showTS(); break;
		/* drop a timeSeries */
		case "DROP" : ExecuteCommand.dropTS(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* exist a timeSeries with name ... */
		case "EXIST" : ExecuteCommand.existTS(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* get a timeSeries */
		case "GET" : ExecuteCommand.getTS(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* create a timeSeries */
		case "CREATE" : ExecuteCommand.createTS2DB(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		
		
		/* ************************************
		 * Time Series
		 *************************************/
		
		/* create the timeSerie schema */
		case "CREATE_SCHEMA" : ExecuteCommand.createSchema(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* show the schema of the timeSerie */
		case "SHOW_SCHEMA" : ExecuteCommand.showSchema(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* get the schema of the timeSerie*/
		case "GET_SCHEMA" : ExecuteCommand.getSchema(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* insert data at a certain file */
		case "INSERT" : ExecuteCommand.insertDataFromFile(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* select timeSerie Range from timeStart to timeEnd */
		case "SELECT_RANGE": break;
		/* select a column of a timeserie */
		case "SELECT": ExecuteCommand.selectColumn(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* find max value of timeSerie */
		case "MAX_VALUE" : ExecuteCommand.maxValue(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* find min value of timeSerie */
		case "MIN_VALUE" : ExecuteCommand.minValue(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* print first five values */
		case "PRINT" : ExecuteCommand.printHead(Arrays.copyOfRange(userCommandLine, 1, size)); break;
				
		
		/* ************************************
		 * Transformations
		 *************************************/
		
		/* power transformation of timeSerie: square root */
		case "SQRT_TRANSFORM" : break;
		/* power transformation of timeSerie: logarithm */
		case "LOG_TRANSFORM" : break;
		/* average of timeSerie */
		case "MEAN" : break;
		/* shifting timeSerie with coefficient */
		case "SHIFT" : break;
		/* scaling timeSerie with coefficient */
		case "SCALE" : break;
		/* standard deviation of timeSeries */
		case "STD_DEVIATION" : break;
		/* normalize the TimeSerie */
		case "NORMALIZE" : break;
		/* search the first time occurs */
		case "SEARCH" : break;
		/* moving average */
		case "MOVING_AVERAGE" : break;
		/* DFT of timeSerie */
		case "DFT" : break;
		/* DTW of 2 timeSerie (similarity between two timeseries) */
		case "DTW" : break;
		
		/* ************************************
		 * Compression 
		 *************************************/

		/* compress a timeserie */
		case "COMPRESSION" : ExecuteCommand.compression(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* decompress a timeserie */
		case "DECOMPRESSION" : break;

		/* ************************************
		 * indexing 
		 *************************************/
		
		/* create index for many ts */
		case "CREATE_INDEX" : ExecuteCommand.createIndex(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* return neighbors of a ts from the create index   */
		case "NEIGHBORS" : break;
		/* ************************************
		 * clustering 
		 *************************************/
		
		/* ************************************
		 * Application 
		 *************************************/
		case "CREATE_DNA" : break;
		case "DNA_SIMILARITY" : ExecuteCommand.dnApplication(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		
				
		default: System.out.println("oups it may have a code error");
		}

		return true;
	}
	
	/*
	 * command help [cmd]
	 * display on the console the explanation and the list of command and option
	 * @param cmd
	 */
	private static void help(String cmd) {
		switch(cmd) {
		case "QUIT" : System.out.println("exit the framework"); break;
		case "EXIT" : System.out.println("exit the framework"); break;
		default: {
			System.out.println("List of commands:\n");
			
			System.out.println("************************************");
			System.out.println("standard commands");
			System.out.println("************************************");
			System.out.println("QUIT 				\t\t quit the application");
			System.out.println("HELP 				\t\t show every command that can be use in PAST application");
			System.out.println("VAR  				\t\t show every variable save in memory that can be use");
			System.out.println("DEL name			\t\t delete the name variable that was save in memory");
			System.out.println("RENAME name newName 	\t\t\t rename the name variable that was save in memeory with the new name");
			System.out.println("STARTSPARK 			\t\t start spark, by default it start at the launch of the Applicationn");	
			System.out.println("STOPSPARK 			\t\t stop spark");
			System.out.println("************************************");
			System.out.println("database commands");
			System.out.println("************************************");
			System.out.println("OPEN database			\t\t open the database but if database dosn't exist, it will create it");
			System.out.println("CLOSE 				\t\t close the database");
			System.out.println("RESTART 			\t\t restart the database");
			System.out.println("SHOW 				\t\t show all Time serie in the database");
			System.out.println("DROP timeserie			\t\t delete the time serie in the database");
			System.out.println("EXIST timserie 			\t\t check if the time serie exist in the database");
			System.out.println("GET timeserie [: name] 	\t\t\t load the timeserie and save in a variable (random or select name) in memory");
			System.out.println("CREATE timeserie FROM file [schema] [: name] \t create a timeserie by loading from a file with optional schema or save in variable with random or select name");
			System.out.println("CREATE timeserie FROM DNA file [: name] \t crreate a special timeserie for DNA (the dna file dosn't need timestampe) ");
			System.out.println("************************************");
			System.out.println("Time Series commands");
			System.out.println("************************************");
			System.out.println("CREATE_SCHEMA [: name] 			\t create a personal schema with deamon tool ");
			System.out.println("SHOW_SCHEMA timeserie 			\t show the schema of a timeserie save in memeory");
			System.out.println("GET_SCHEMA FROM timeserie [: name] 	\t get the schema of a timeserie and save in memeory ");
			/* System.out.println("INSERT"); */ 
			/* System.out.println("SELECT RANGE"); */
			System.out.println("SELECT column FROM timeserie [: name] \t\t get the RDD of the column of the timeserie and save in variable name");
			System.out.println("MAX_VALUE colum FROM timeserie 		\t find the max value of colum of the timeserie ");
			System.out.println("MAX_VALUE rdd 				\t find the max value of the RDD ");
			System.out.println("MIN_VALUE colum FROM timeserie 		\t find the min value of colum of the timeserie ");
			System.out.println("MAX_VALUE rdd 				\t find the min value of the RDD ");
			System.out.println("PRINT FROM timeserie			\t print the 10 first values of the timeserie");
			System.out.println("PRINT FROM rdd				\t print the 10 first values of the RDD");
			System.out.println("************************************");
			System.out.println("Transformation commands");
			System.out.println("************************************");
			/* System.out.println("SQRT TRANSFORM"); */
			/* System.out.println("LOG TRANSFORM"); */
			/* System.out.println("MEAN"); */
			/* System.out.println("SHIFT"); */
			/* System.out.println("SCALE"); */
			/* System.out.println("STD DEVIATION"); */
			/* System.out.println("NORMALIZE"); */
			/* System.out.println("SEARCH"); */
			/* System.out.println("MOVING AVERAGE"); */
			System.out.println("************************************");
			System.out.println("Compression commands");
			System.out.println("************************************");
			// System.out.println("COMPRESSION timeserie WITH {regression, APCA, demon} \t compress the timeserie with one parameter");
			System.out.println("************************************");
			System.out.println("indexing");
			System.out.println("************************************");
			System.out.println("CREATE_INDEX 				\t execute a deamon tool to assist of the creation of the index");
			System.out.println("CREATE_INDEX FROM timeserie timeserie ..\t create index with the select timeserie");
			System.out.println("************************************");
			System.out.println("clustering");
			System.out.println("************************************");
			System.out.println("");
			System.out.println("************************************");
			System.out.println("Forecasting");
			System.out.println("************************************");
			System.out.println("");
			System.out.println("************************************");
			System.out.println("Application");
			System.out.println("************************************");
			System.out.println("DNA_SIMILARITY BETWEEN dna1 IN dna2	\t find the most similar part of dna1 in the dna2");
			System.out.println("------------------------------------");
			System.out.println("");
			System.out.println("Example: we want to find the most similar DNA part of one file in a other file. And we have no database.");
			System.out.println(" 1: OPEN db");
			System.out.println(" 2: CREATE dna1 FROM DNA dna1.txt : TSchimpDNA");
			System.out.println(" 3: CREATE dna2 FROM DNA dna2.txt : TSbigDNA");
			System.out.println(" 4: SELECT data FROM TSchimpDNA : chimpDNA");
			System.out.println(" 5: SELECT data FROM TSbigDNA : bigDNA");
			System.out.println(" 6: DNA_SIMILARITY BETWEEN chimpDNA IN bigDNA");
			System.out.println("");
			System.out.println("");
		}
		}
		
	}

}
