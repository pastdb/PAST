package past;

public class Message {
	
	/*
	 * call message
	 */
	public static void intro() {
		System.out.println("Welcome to PAST (Processing And Storage of Time series) \nBig Data Project v0.1 \n"
				+ "Type help for more information.");
	}
	
	public static void unknowCommand() {
		System.out.println("unknown command");
	}
	
	public static void invalidChar() {
		System.out.println("invalid char");
	}
	
	public static void goodbye() {
		System.out.println("goodbye");
	}
	
	public static void errorConsole() {
		System.err.println("No console.");
	}
	

}
