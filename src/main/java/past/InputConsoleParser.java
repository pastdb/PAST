package past;


/**
 * Parse a String of the user input and return an array of 
 * each element on the string.
 */
public class InputConsoleParser {


	public static String[] parser(String userCommand) {

		// save on one variable or not "="
		// execute fonction "(...)"
		// enter to valid the command

		// char allow abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ 1234567890 = () , 
		String tmp = userCommand;

		tmp = tmp.replace("=", " = ");
		tmp = tmp.replace(",", " , ");
		tmp = tmp.replace("(", " ( ");
		tmp = tmp.replace(")", " ) ");
		tmp = tmp.replaceAll("\\s+"," ");
		//System.out.println(tmp);
		String tab[] = tmp.split(" ");
		
		return tab;
	}

}
