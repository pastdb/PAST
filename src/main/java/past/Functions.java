

package past;

import java.util.Hashtable;

import past.storage.DBType;

/**
 * implementation of the basic function that can be applied on the Timeseries
 * 
 */
public class Functions {

	/**
	 * finds the min in the Timeseries for a certain attribute
	 * 
	 * @param ts
	 * @param attribute
	 */
	public Double min(Timeseries ts, String attribute) {

		Double min = Double.MAX_VALUE;

		Hashtable<Integer, DBType> data = ts.getTimeseries().get(attribute);

		for (Integer i : data.keySet()) {

			if (data.get(i) > min) {
				min = data.get(i);
			}

		}

		return min;

	}

	/**
	 * finds the max in the Timeseries for a certain attribute
	 * 
	 * @param ts
	 * @param attribute
	 */
	public Double max(Timeseries ts, String attribute) {

		Double max = Double.MIN_VALUE;

		Hashtable<Integer, DBType> data = ts.getTimeseries().get(attribute);

		for (Integer i : data.keySet()) {

			if (data.get(i) > max) {
				max = data.get(i);
			}

		}

		return max;

	}

	/**
	 * returns the range of the dimension in the Timseries
	 * 
	 * @param ts
	 * @param attribute
	 */
	public Double range(Timeseries ts, String attribute) {

		Double min = min(ts, attribute);
		Double max = max(ts, attribute);

		return max - min;
	}

}
