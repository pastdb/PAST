package past;

import org.apache.spark.api.java.*;

import java.util.HashMap;
import java.util.Map;

/**
 * this class will execute command that user want
 *
 */
public class Command {

	/*  List of commands available on the console */
	static String commandsList[] = {"quit", "exit", "help", "load", "memList", "wordcount"};
	
	/* variable save by the user */
	static Map<String, Object > map_variable = new HashMap<String, Object >();
	
	/* spark context */
//	static JavaSparkContext sc = new JavaSparkContext("local", "PAST");
	
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
		
		/*
		 * standard commands
		 */
		case "quit": return false;
		case "exit": return false;
		case "help": help( (size > 1) ? userCommandLine[1].trim() : ""); break;
		case "memList": {
			for (String key: map_variable.keySet()) {
			    System.out.println("key : " + key + " - value : " + map_variable.get(key));
			}
		} break;
		
		/*
		 * related with load/save data commands
		 */
		case "load": {
			if(size != 3) System.out.println("Usage: load <file> <save in variable>");
			else {
//				map_variable.put(userCommandLine[2].trim(), sc.textFile(userCommandLine[1].trim()));
			}
		} break;
		
		
		
		/*
		 * Processing And Storage of Time series commands 
		 */
		
		/* Time Series */
		/* TODO */
		case "Timeseries" : break;
		case "addAttribute" : break;
		case "getAttribute" : break;
		case "removeAttrivute" : break;
		case "addValue" : break;
		case "getValue" : break;
		case "removeValue" : break;
		case "getTimeseries" : break;
		
		/* Transformations*/
		/* TODO */
		case "sqrtTransform" : break;
		case "logTransform" : break;
		case "mean" : break;
		case "subtractMean" : break;
		case "range" : break;
		case "extractFrame" : break;
		case "mode" : break;
		case "shift" : break;
		case "scale" : break;
		case "stdDeviation" : break;
		case "normalize" : break;
		case "binarySearch" : break;
		case "movingAverageSmoother" : break;
		case "piecewiseAggregateApproximation" : break;
		case "symolicAggregateApproximation" : break;
		case "DFT" : break;
		
		/* Complex */
		/* TODO */
		case "Complex" : break;
		case "toString" : break;
		
		/* Functions */
		/* TODO */
		case "min" : break;
		case "max" : break;
		
		
		
		/*
		 * Test for spark commands
		 */
		case "wordcount": {
			if(size != 2) System.out.println("Usage: wordcount <save in variable>");
			else if(map_variable.containsKey(userCommandLine[1].trim())) {
//				WordCount.wordcount(map_variable.get(userCommandLine[1]));
			}
			else System.out.println("unknown variable");
		} break; 
		
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
			System.out.println("Available commands ");
			String tmp = "";
			for(String s: commandsList) tmp += s + " ";
			System.out.println("Commands List: " + tmp);
		}
		}
	}

}
