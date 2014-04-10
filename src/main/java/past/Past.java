package past;

import java.io.Console;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * The Main Class of the framework Processing And Storage of Time series.
 * 
 * This class, it's allow user to interact with the console to apply several function 
 * in relation of time series. 
 * @author ntran
 *
 */
public class Past {

	public static void main(String[] args) {
		
		/* 
		 * Past can only be use in console 
		 */
		Console con = System.console();
		if(con == null) {
			Message.errorConsole();
            System.exit(1);
		}
		
		/*
		 *    
		 */
		boolean isContinue = true;
		String userCommandLine[] = null;
		String userCommand = null;
		String commandsList[] = Command.commandsList;
		ArrayList<String> validCommand = new ArrayList<String>(Arrays.asList(commandsList));
		
		/* 
		 * welcome message 
		 */
		Message.intro();
		
		/*  
		 * main part of this class.
		 * it parses user'command and divide in word to put in an array and lunch the command.  
		 */
		do{
			userCommandLine = trimString(con.readLine("past>  ").trim().split(" "));
			
			userCommand = userCommandLine[0];
			if(userCommand.length() == 0) {
				/* do nothing */
			}
			else if(validCommand.contains(userCommand)) {
				isContinue = Command.executeCommand(userCommandLine);
			}
			else {
				Message.unknowCommand();
			}	
		} while(isContinue);
		
		/* 
		 * goodbye message and exit 
		 */
		Message.goodbye();
		System.exit(0);
	}
	
	/*
	 * for each String, it take out some space before and after a word
	 */
	private static String[] trimString(String[] tab) {
		String[] tmp = tab;
		for(int i=0; i<tab.length; i++) {
			tmp[i] = tab[i].trim();
		}
		return tmp;
	}
}
