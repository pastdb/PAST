package past;

import java.util.Hashtable;

/**
 * implementation of the basic function that can be applied on the Timeseries
 *
 */
public class Functions {

	/**
	 * finds the min in the Timeseries for a certain dimension
	 * 
	 * @param ts
	 * @param dimension
	 */
	public Double min(Timeseries ts, String dimension) {

		Double min = Double.MAX_VALUE;

		Hashtable<Integer, Double> data = ts.getTimeseries().get(dimension);

		for (Integer i : data.keySet()) {

			if (data.get(i) > min) {
				min = data.get(i);
			}

		}

		return min;

	}

	/**
	 * finds the max in the Timeseries for a certain dimension
	 * 
	 * @param ts
	 * @param dimension
	 */
	public Double max(Timeseries ts, String dimension) {

		Double max = Double.MIN_VALUE;

		Hashtable<Integer, Double> data = ts.getTimeseries().get(dimension);

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
	 * @param dimension
	 */
	public Double range(Timeseries ts, String dimension) {
		
		Double min=min(ts, dimension);
		Double max=max(ts, dimension);
		
		return max-min;
	}

}
