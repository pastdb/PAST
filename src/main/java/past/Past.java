package past;

import java.io.Console;

/**
 * The Main Class of the framework Processing And Storage of Time series.
 * 
 * This class, it's allow user to interact with the console to apply several function 
 * in relation of time series. 
 * 
 *
 */
public class Past {

	public static void main(String[] args) {
		
		/* 
		 * Past can only be use in console 
		 */
		Console con = System.console();
		if(con == null) {
			System.err.println("No console.");
            System.exit(1);
		}
		
		/* 
		 * welcome message 
		 */
		System.out.println("************ WELCOME TO *************\n");
		System.out.println("      ****  ****  **** *****");
		System.out.println("      *  *  *  *  *      *");
		System.out.println("      ****  ****  ****   *");
		System.out.println("      *     *  *     *   *");
		System.out.println("      *     *  *  ****   *");
		System.out.println("Processing And Storage of Time series");
		System.out.println("Big Data Project v0.1 \n\n --> Type help for more information.\n\n");
		
		/*
		 * isContinue: console continue until quit 
		 * userInput: userInput string 
		 */
		final String VALID_CHAR = "[a-zA-Z0-9,=:()'\\s-]*";
		boolean isContinue = true;
		String userInput = null;
		
		/*  
		 * main part of this class.
		 * it parses user'command and divide in word to put in an array and lunch the command.  
		 */
		do{
			userInput = con.readLine("past>  ").trim();
			if(!userInput.matches(VALID_CHAR)) {
				System.out.println("invalid char");
			}
			else {
				isContinue = Command.executeCommand(InputConsoleParser(userInput));
			}
		} while(isContinue);
		
		/* 
		 * goodbye message and exit 
		 */
		System.out.println("GOODBYE");
		System.exit(0);
	}
	
	/*
	 * for each String, it take out some space before and after a word
	 */
	private static String[] InputConsoleParser(String userInput) {
		String tmp = userInput.trim();
		tmp = tmp.replace("=", " = ");
		tmp = tmp.replace(":", " : ");
		tmp = tmp.replace(",", " , ");
		tmp = tmp.replace("(", " ( ");
		tmp = tmp.replace(")", " ) ");
		tmp = tmp.replaceAll("\\s+"," ");
		return tmp.split(" ");
	}
}
