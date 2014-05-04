package past;

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
		"quit", "exit", "help", "var",
		/* database */
		"USE", "OPEN", "CLOSE", "SHOW", "DROP", "EXIST", "GET", "CREATE",
		/* Time Serie */
		"CREATE TS SCHEMA", "SHOW TS SCHEMA", "INSERT", "SELECT RANGE", "MAX VALUE", "MIN VALUE",
		/* Transformations */
		"SQRT TRANSFORM", "LOG TRANSFORM", "MEAN", "SHIFT", 
		"SCALE", "STD DEVIATION", "NORMALIZE", "SEARCH", "MOVING AVERAGE", "DFT",
		/* Compression */
		
		/* indexing */
		
		/* clustering */
		
		/* Forecasting */
	};
	/* help to use contain */
	static private ArrayList<String> commandsList = new ArrayList<String>(Arrays.asList(commands));
	
	/* variable save by the user */
	static Map<String, Object > map_variable = new HashMap<String, Object >();
	
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
		else if(!(commandsList.contains(userInput[0]) || (size > 2 && commandsList.contains(userInput[2])))) {
			System.out.println("unknown command");
		}
		else if(size > 1 && userInput[1].compareTo("=") == 0) { // save in variable
			
			String saveVar = userInput[0];
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

		switch(userCommandLine[0].trim()) {
		
		/* ************************************
		 * standard commands
		 *************************************/
		
		case "quit": return false;
		case "exit": return false;
		//case "ls" : break;
		case "help": help( (size > 1) ? userCommandLine[1].trim() : ""); break;
		case "var": {
			for (String key: map_variable.keySet()) {
			    System.out.println("key : " + key + " - value : " + map_variable.get(key));
			}
		} break;
		
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
		case "CREATE TS SCHEMA" : break;
		/* show the schema of this timeSerie */
		case "SHOW TS SCHEMA" : break;
		/* insert data at a certain column */
		case "INSERT" : break;
		/* select timeSerie Range from timeStart to timeEnd */
		case "SELECT RANGE": break;
		/* find max value of timeSerie */
		case "MAX VALUE" : break;
		/* find min value of timeSerie */
		case "MIN VALUE" : break;
		
		
		/* ************************************
		 * Transformations
		 *************************************/
		
		/* power transformation of timeSerie: square root */
		case "SQRT TRANSFORM" : break;
		/* power transformation of timeSerie: logarithm */
		case "LOG TRANSFORM" : break;
		/* average of timeSerie */
		case "MEAN" : break;
		/* shifting timeSerie with coefficient */
		case "SHIFT" : break;
		/* scaling timeSerie with coefficient */
		case "SCALE" : break;
		/* standard deviation of timeSeries */
		case "STD DEVIATION" : break;
		/* normalize the TimeSerie */
		case "NORMALIZE" : break;
		/* search the first time occurs */
		case "SEARCH" : break;
		/* moving average */
		case "MOVING AVERAGE" : break;
		//case "piecewiseAggregateApproximation" : break;
		//case "symolicAggregateApproximation" : break;
		
		/* DFT of timeSerie */
		case "DFT" : break;
		
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
			System.out.println("CREATE TS SCHEMA");
			System.out.println("SHOW TS SCHEMA");
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
