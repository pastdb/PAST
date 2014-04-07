// Thomas Mühlematter

package past.example;

import java.util.Hashtable;

import past.Timeseries;
import past.storage.DBType;


/**
 * This class presents different usage and access scenarios of Timeseries. It is
 * used as a reference of how to communicate with time series.
 * 
 */
public class TimeseriesExamples {
	
	public static void main(String[] args) {
		
		// we create a new hashtable to represent an attribute of the time series
		Hashtable<Integer, DBType> attribute = new Hashtable<Integer,DBType>();
		attribute.put(0, new DBType(5));
		attribute.put(1, new DBType(1));
		attribute.put(2, new DBType(2));
		
		
		Timeseries ts = new Timeseries();
		System.out.println(ts.addAttribute("velocity", attribute)); // returns true
		System.out.println(ts.addAttribute("velocity", attribute)); // returns false as the attribute already exists in the TS
		
		ts.addValue("velocity", 3, new DBType(3));
		System.out.println(ts.getValue("velocity", 3)); // returns 3
		ts.removeValue("velocity", 3);
		System.out.println(ts.getValue("velocity", 3)); // returns null as the value doesn't exist anymore
		
		ts.removeAttribute("velocity");
		System.out.println(ts.getAttribute("velocity")); // returns null as the attribute doesn't exist anymore
		
		
	}
}
