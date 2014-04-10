package past;

import org.apache.spark.api.java.*;

import java.util.HashMap;
import java.util.Map;


public class Command {

	/*  List of commands available on the console */
	static String commandsList[] = {"quit", "exit", "help", "load", "memList", "wordcount"};
	
	/*  */
	static Map<String, JavaRDD<String> > map_variable = new HashMap<String, JavaRDD<String> >();
	
	/* spark context */
	static JavaSparkContext sc = new JavaSparkContext("local", "PAST");
	
	/**
	 * 
	 * @param userCommandLine
	 * @return
	 */
	public static boolean executeCommand(String userCommandLine[]) {
		int size = userCommandLine.length;

		switch(userCommandLine[0].trim()) {
		case "quit": return false;
		case "exit": return false;
		case "help": help( (size > 1) ? userCommandLine[1].trim() : ""); break;
		
		case "memList": {
			for (String key: map_variable.keySet()) {
			    System.out.println("key : " + key + " - value : " + map_variable.get(key));
			}
		} break;
		
		case "load": {
			if(size != 3) System.out.println("Usage: load <file> <save in variable>");
			else {
				map_variable.put(userCommandLine[2].trim(), sc.textFile(userCommandLine[1].trim()));
			}
		} break;
		
		case "wordcount": {
			if(size != 2) System.out.println("Usage: wordcount <save in variable>");
			else if(map_variable.containsKey(userCommandLine[1].trim())) {
				WordCount.wordcount(map_variable.get(userCommandLine[1]));
			}
			else System.out.println("unknown variable");
		} break; 
		
		default: System.out.println("oups it may have a code error");
		}

		return true;
	}

	/**
	 * command help [cmd]
	 * display on the console the explanation and the list of command and option
	 * @param cmd
	 */
	public static void help(String cmd) {
		switch(cmd) {
		case "quit" : System.out.println("exit the framework"); break;
		case "exit" : System.out.println("exit the framework"); break;
		case "load" : System.out.println("  load take parametter ");
		default: {
			System.out.println("help description to do ... ");
			String tmp = "";
			for(String s: commandsList) tmp += s + " ";
			System.out.println("Commands List: " + tmp);
		}
		}
	}

}
