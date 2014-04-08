package past;

import java.io.Console;
import java.util.ArrayList;
import java.util.Arrays;

public class Past {

	public static void main(String[] args) {
		
		Console con = System.console();
		if(con == null) {
			Message.errorConsole();
            System.exit(1);
		}
		
		boolean isContinue = true;
		String userCommandLine[] = null;
		String userCommand = null;
		String commandsList[] = Command.commandsList;
		ArrayList<String> validCommand = new ArrayList<String>(Arrays.asList(commandsList));
		
		Message.intro();
		
		do{
			userCommandLine = con.readLine("past>  ").trim().split(" ");
			userCommand = userCommandLine[0].trim();
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
		
		Message.goodbye();
		System.exit(0);

	}
}
