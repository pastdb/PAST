package past;

import java.util.Hashtable;
import past.storage.DBType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import com.typesafe.config.Config; 
import com.typesafe.config.ConfigFactory;
/**
 * represents a multi-dimension time series
 * 
 */

public class Timeseries {

	private Hashtable<String, Hashtable<Integer, Object>> data;
	private DBType.DBType<?> type;
	private Path path;
	private String name;
	
	String containingPath = "pastdb_timeseries_"+java.lang.System.nanoTime();
	/**
	 * constructor with outside data
	 * 
	 * @param data
	 */
	public Timeseries(Hashtable<String, Hashtable<Integer, Object>> data, DBType.DBType<?> type) {

		this.data = data;
		this.type = type;
		
	}
	
	public Timeseries(String name,  Hashtable<String, Hashtable<Integer, Object>> data, DBType.DBType<?> type) {
		this(data,type);
		this.name=name;

	}
	public String getName(){
		return name;
	}
	public void setName(String name ){
		this.name = name;
	}
	public void setPath(Path containingPath){
	
		this.path = new Path(containingPath, name);
	}
	public Path getPath(){
		return this.path;
	}
	

	/**
	 * constructor for a new Timeseries
	 */
	public Timeseries() {
		this.data = new Hashtable<String, Hashtable<Integer, Object>>();
	}

	/**
	 * adds a new attribute to the time series
	 * 
	 * @return false if attribute already exists in Timeseries 0 if attribute
	 *         added correctly true if added correctly
	 */
	public boolean addAttribute(String key, Hashtable<Integer, Object> attribute) {
		if (data.get(key) != null) {
			return false;
		} else {
			this.data.put(key, attribute);
			return true;
		}
	}

	/**
	 * adds a new attribute to the time series
	 * 
	 * @return null if the attribute doesn't exist
	 */
	public Hashtable<Integer, Object> getAttribute(String key) {

		return this.data.get(key);

	}

	/**
	 * remove an attribute from the time series
	 * 
	 * @return false if attribute doesn't exist in the time series true if the
	 *         attribute was removed correctly
	 */
	public boolean removeAttribute(String key) {
		if (data.get(key) != null) {
			data.remove(key);
			return true;
		} else {

			return false;
		}
	}

	/**
	 * adds a value to a certain attribute and creates it if it didn't exist
	 * 
	 * @param key
	 * @param timestep
	 * @param value
	 */
	public void addValue(String key, Integer timestep, Object value) {

		Hashtable<Integer, Object> attribute = data.get(key);

		if (attribute == null) {
			attribute = new Hashtable<Integer, Object>();
		}

		attribute.put(timestep, value);
		this.data.put(key, attribute);
	}

	/**
	 * gets a certain value from a given attribute for a certain timestep
	 * 
	 * @param key
	 * @param timestep
	 * 
	 * @return null if the attribute or the timestep doesn't exist
	 */
	public Object getValue(String key, Integer timestep) {

		Hashtable<Integer, Object> attribute = data.get(key);

		if (attribute == null) {
			return null;
		}

		Object result = attribute.get(timestep);

		return result;
	}

	/**
	 * removes a value in a certain attribute
	 * 
	 * @param key
	 * @param timestep
	 * 
	 * @return false if the attribute or the timestep didn't exist true if the
	 *         timestep was removed correctly
	 */
	public boolean removeValue(String key, Integer timestep) {

		Hashtable<Integer, Object> attribute = data.get(key);

		if (attribute == null) {
			return false;
		} else {
			if (attribute.get(timestep) == null) {
				return false;
			}
			attribute.remove(timestep);
			this.data.put(key, attribute);
			return true;

		}
	}

	/**
	 * returns the Timeseries
	 */
	public Hashtable<String, Hashtable<Integer, Object>> getTimeseries() {
		return this.data;
	}

}

