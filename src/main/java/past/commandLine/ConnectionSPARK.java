package past;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * Initialize and get the JavaSparkContext
 * ConnectionSPARK use Singleton pattern
 * @author ntran
 *
 */
public class ConnectionSPARK {
	
	private static ConnectionSPARK connection = null;
	private static JavaSparkContext sc;
	private String appName = "PAST";
	private String master = "local";
	
	
	private ConnectionSPARK(String master) {
		try {
			this.master = master;
			sc = new JavaSparkContext(this.master, this.appName);
		}
		catch (Exception e) {
			System.out.println("Connection Spark Error: " + e);
		}
	}
	
	/**
	 * get JavaSparkContext. Before use this function, be sure to use first initialize function. 
	 * @return JavaSparkContext
	 */
	public static synchronized JavaSparkContext getSparkContext() {
		if(connection == null) {
			throw new IllegalStateException("not initialized");
		} 
		else return sc;
	}
	
	/**
	 * Initialize JavaSparkContext
	 * @param master
	 */
	public static synchronized void initialize(String master) {
		if(connection != null) {
			throw new IllegalStateException("already initialized");
		}
		connection = new ConnectionSPARK(master);
	}

}
