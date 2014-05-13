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
		"QUIT", "EXIT", "HELP", "VAR", "STOPSPARK", "STARTSPARK",
		/* database */
		"USE", "OPEN", "CLOSE", "SHOW", "DROP", "EXIST", "GET", "CREATE",
		/* Time Serie */
		"CREATE_SCHEMA", "SHOW_SCHEMA", "GET_SCHEMA", "INSERT", "SELECT_RANGE", "MAX_VALUE", "MIN_VALUE",
		"SELECT", 
		/* Transformations */
		"SQRT TRANSFORM", "LOG TRANSFORM", "MEAN", "SHIFT", 
		"SCALE", "STD DEVIATION", "NORMALIZE", "SEARCH", "MOVING AVERAGE", "DFT",
		/* Compression */
		"COMPRESSION", "DECOMPRESSION",
		/* indexing */
		"GET_INDEXING",
		/* clustering */
		
		/* Forecasting */
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
		
		/* ************************************
		 * standard commands
		 *************************************/
		
		case "QUIT": return false;
		case "EXIT": return false;
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
		case "USE" : break;
		case "OPEN" : ExecuteCommand.openDB(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* close database */
		case "CLOSE" : ExecuteCommand.closeDB(); break;
		/* show list of TimeSeries */
		case "SHOW" : ExecuteCommand.showTS(); break;
		/* drop a timeSeries */
		case "DROP" : ExecuteCommand.dropTS(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* exist a timeSeries with name ... */
		case "EXIST" : ExecuteCommand.existTS(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* get a timeSeries */
		case "GET" : ExecuteCommand.getTS(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		/* create a timeSeries */
		case "CREATE" : ExecuteCommand.createTS(Arrays.copyOfRange(userCommandLine, 1, size)); break;
		
		
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
		/* find min timestamp of timeSerie */
		case "MAX_TIMESTAMP" : break;
		/* find max timestamp of timeSerie */
		case "MIN_TIMESTAMP" : break;
		
		
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
		//case "piecewiseAggregateApproximation" : break;
		//case "symolicAggregateApproximation" : break;
		
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
		case "SET_INDEXING" : break;
		case "GET_INDEXING" : break;
		/* ************************************
		 * clustering 
		 *************************************/
		
		/* ************************************
		 * Application 
		 *************************************/

		
				
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
		case "quit" : System.out.println("exit the framework"); break;
		case "exit" : System.out.println("exit the framework"); break;
		case "load" : System.out.println("  load take parametter ");
		default: {
			System.out.println("List of commands:\n");
			
			System.out.println("************************************");
			System.out.println("standard commands");
			System.out.println("************************************");
			System.out.println("quit");
			System.out.println("help");
			System.out.println("var");
			System.out.println("startSpark");
			System.out.println("stopSpark");
			System.out.println("************************************");
			System.out.println("database commands");
			System.out.println("************************************");
			System.out.println("OPEN");
			System.out.println("CLOSE");
			System.out.println("SHOW");
			System.out.println("DROP");
			System.out.println("EXIST");
			System.out.println("GET");
			System.out.println("CREATE");
			System.out.println("************************************");
			System.out.println("Time Series commands");
			System.out.println("************************************");
			System.out.println("CREATE_SCHEMA");
			System.out.println("SHOW_SCHEMA");
			System.out.println("GET_SCHEMA");
			System.out.println("INSERT");
			System.out.println("SELECT RANGE");
			System.out.println("MAX VALUE");
			System.out.println("MIN VALUE");
			System.out.println("************************************");
			System.out.println("Transformation commands");
			System.out.println("************************************");
			System.out.println("SQRT TRANSFORM");
			System.out.println("LOG TRANSFORM");
			System.out.println("MEAN");
			System.out.println("SHIFT");
			System.out.println("SCALE");
			System.out.println("STD DEVIATION");
			System.out.println("NORMALIZE");
			System.out.println("SEARCH");
			System.out.println("MOVING AVERAGE");
			System.out.println("************************************");
			System.out.println("Compression commands");
			System.out.println("************************************");
			System.out.println("");
			System.out.println("************************************");
			System.out.println("indexing");
			System.out.println("************************************");
			System.out.println("");
			System.out.println("************************************");
			System.out.println("clustering");
			System.out.println("************************************");
			System.out.println("");
			System.out.println("************************************");
			System.out.println("Forecasting");
			System.out.println("************************************");
			System.out.println("");
			System.out.println("");
		}
		}
		
	}

}
