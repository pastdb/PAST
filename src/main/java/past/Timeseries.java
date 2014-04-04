package past;

import java.util.Hashtable;

/**
 * represents a multi-dimension time series
 * 
 */
public class Timeseries {

	private Hashtable<String, Hashtable<Integer, Double>> data;

	/**
	 * constructor when data are available
	 * 
	 * @param data
	 */
	public Timeseries(Hashtable<String, Hashtable<Integer, Double>> data) {

		this.data = data;
	}

	/**
	 * constructor for a new Timeseries
	 */
	public Timeseries() {
		this.data = new Hashtable<String, Hashtable<Integer, Double>>();
	}

	/**
	 * adds a new dimension to the time series
	 * 
	 * @param dimension
	 */
	public void addDimension(String key, Hashtable<Integer, Double> dimension) {
		this.data.put(key, dimension);
	}
	
	/**
	 * adds a value to a certain dimension and creates it if it didn't exist 
	 * 
	 * @param key
	 * @param time
	 * @param value
	 */
	public void addValue(String key, Integer time, Double value){
		
		Hashtable<Integer, Double> dimension=data.get(key);
		
		if(dimension==null){
			dimension=new Hashtable<Integer, Double>();
		}
		
		dimension.put(time, value);
		this.data.put(key, dimension);	}

	/**
	 * returns the Timeseries
	 */
	public Hashtable<String, Hashtable<Integer, Double>> getTimeseries() {
		return this.data;
	}

}
