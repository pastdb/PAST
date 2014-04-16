package past;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
//import past.storage.DBType;


public class Transformations {
	//TODO: change all Doubles to DBType ?
	/*
	 * implements different functions and transformations of the time series
	 */
	
	public static Double epsilon = 0.00000001; // assume Double as equal

	
	/*
	 * Power transformation of time series: square root (on complete data set 
	 * or partial window frame with start and end time given)
	 * @param tsData time series
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return resultTsData transformed time series
	 */
	public static Hashtable<Integer, Double> sqrtTransform(Hashtable<Integer, Double> tsData, int timeStart, int timeEnd) {
		Hashtable<Integer, Double> resultTsData = new Hashtable<Integer, Double>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				Double temp = Math.sqrt(tsData.get(i));
				resultTsData.put(i, temp);
			}
		}
		return resultTsData;
	}

	
	/*
	 * Power transformation of time series: logarithm (on complete data set 
	 * or partial window frame with start and end time given)
	 * @param tsData time series
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return resultTsData transformed time series
	 */
	public static Hashtable<Integer, Double> logTransform(Hashtable<Integer, Double> tsData, int timeStart, int timeEnd) {
		Hashtable<Integer, Double> resultTsData = new Hashtable<Integer, Double>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				Double temp = Math.log(tsData.get(i));
				resultTsData.put(i, temp);
			}
		}
		return resultTsData;
	}
	
	
	/*
	 * Compute average of time series (on complete data set 
	 * or partial window frame with start and end time given)
	 * @param tsData time series
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return avg average of time series
	 */
	public static Double mean(Hashtable<Integer, Double> tsData, int timeStart, int timeEnd) {
		Double avg = .0;
		int count = 0;
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				avg += tsData.get(i);
				count ++;
			}
		}
		avg /= count;
		return avg;
	}

	
	/*
	 * Compute the average on interval and subtract the average from original values
	 * @param tsData time series
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return resultTsData transformed time series
	 */
	public static Hashtable<Integer, Double> subtractMean(Hashtable<Integer, Double> tsData, int timeStart, int timeEnd) {
		Hashtable<Integer, Double> resultTsData = new Hashtable<Integer, Double>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		double average = mean(tsData, timeStart, timeEnd);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				resultTsData.put(i, tsData.get(i)-average);
			}
		}
		return resultTsData;
	}
	
	
	/*
	 * Range of the time series: highest value - lowest value
	 * @param tsData time series
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return range time series range on interval
	 */
	public static Double range(Hashtable<Integer, Double> tsData, int timeStart, int timeEnd) {
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Double min = tsData.get(timeStart);
		Double max = tsData.get(timeStart);
		
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				if (tsData.get(i) < min) min = tsData.get(i);
				if (tsData.get(i) > max) max = tsData.get(i);
			}
		}
		
		Double range = max - min;
		return range;
	}
	

	/*
	 * Helper function to extract only partial values from the time series
	 * @param tsData time series
	 * @param timeStart start time of frame
	 * @param timeEnd end time of frame
	 * @return resultFrame values extracted from time series
	 */
	public static ArrayList<Double> extractFrame(Hashtable<Integer, Double> tsData, int timeStart, int timeEnd) {
		ArrayList<Double> resultFrame = new ArrayList<Double>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i > timeStart && i < timeEnd) {
				resultFrame.add(tsData.get(i));
			}
		}
		return resultFrame;
	}

	
	/*
	 * Compute mode of time series (on complete data set 
	 * or partial window frame with start and end time given)
	 * @param tsData time series
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return maxVal mode of the time series
	 */
	public static Double mode(Hashtable<Integer, Double> tsData, int timeStart, int timeEnd) {
		ArrayList<Double> sortedData = extractFrame(tsData, timeStart, timeEnd);
		Collections.sort(sortedData);

		Double maxVal = sortedData.get(0);
		int maxOccur = 0, tempMaxOccur = 0, i = 0;
		Double tempMaxVal = maxVal;
		
		while (i < sortedData.size()) {
			while (i < sortedData.size() && sortedData.get(i) - tempMaxVal < epsilon) {
				tempMaxOccur ++;
				i++;
			}
			if (tempMaxOccur > maxOccur) {
				maxOccur = tempMaxOccur;
				maxVal = sortedData.get(i-1);
			}
			if (i < sortedData.size() - 1) {
				tempMaxVal = sortedData.get(i);
				tempMaxOccur = 0;
			}
			i++;
		}
		return maxVal;
	}	

	
	/*
	 * Shifting time series (window) with coefficient
	 * @param tsData time series
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @param coeff coefficient to shift with
	 * @return resultTsData shifted time series
	 */
	public static Hashtable<Integer, Double> shift(Hashtable<Integer, Double> tsData, int timeStart, int timeEnd, int coeff) {
		Hashtable<Integer, Double> resultTsData = new Hashtable<Integer, Double>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				resultTsData.put(i, tsData.get(i)+coeff);
			}
		}
		return resultTsData;
	}

	
	/*
	 * Scaling time series (window) with coefficient
	 * @param tsData time series
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @param coeff coefficient to scale with
	 * @return resultTsData scaled time series
	 */
	public static Hashtable<Integer, Double> scale(Hashtable<Integer, Double> tsData, int timeStart, int timeEnd, int coeff) {
		Hashtable<Integer, Double> resultTsData = new Hashtable<Integer, Double>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				resultTsData.put(i, tsData.get(i)*coeff);
			}
		}
		return resultTsData;
	}
	
	
	/*
	 * Standard deviation of the time series
	 * @param tsData time series
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @return STD standard deviation for time series interval
	 */
	public static Double stdDeviation(Hashtable<Integer, Double> tsData, int timeStart, int timeEnd) {
		double avg = mean(tsData, timeStart, timeEnd);
		double STD = 0;
		int count = 0;
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				STD += (tsData.get(i)-avg)*(tsData.get(i)-avg);
				count ++;
			}
		}
		STD = Math.sqrt(STD/count);
		return STD;
	}
	
	
	/*
	 * Normalization of time series
	 * @param tsData time series
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @return resultTsData normalized time series
	 */
	public static Hashtable<Integer, Double> normalize(Hashtable<Integer, Double> tsData, int timeStart, int timeEnd) {
		double avg = mean(tsData, timeStart, timeEnd);
		double std = stdDeviation(tsData, timeStart, timeEnd);
		Hashtable<Integer, Double> resultTsData = new Hashtable<Integer, Double>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				resultTsData.put(i, (tsData.get(i)-avg)/std);
			}
		}
		return resultTsData;
	}


	/*
	 * If only window frame is needed, use binary search to find
	 * position of where the first time occurs
	 * @param searchData values to search through - usually time values
	 * @param timeStart first time of the currently needed time series
	 * @return middle position for the required time series start
	 */
	public static int binarySearch(ArrayList<Integer> searchData, int timeStart) {
		int first = 0;
		int last = searchData.size() - 1;
		int middle = (first + last)/2;
		
		while (first <= last) {
			if (searchData.get(middle) < timeStart) {
				first = middle + 1;
			}
			else if (searchData.get(middle) == timeStart) {
				return middle;
			}
			else {
				last = middle - 1;
			}
			middle = (first + last)/2;
		}
		return middle;	
	}

	public static int binarySearch(ArrayList<Double> searchData, Double timeStart) {
		int first = 0;
		int last = searchData.size() - 1;
		int middle = (first + last)/2;
		
		while (first <= last) {
			if (searchData.get(middle) < timeStart) {
				first = middle + 1;
			}
			else if (searchData.get(middle) == timeStart) {
				return middle;
			}
			else {
				last = middle - 1;
			}
			middle = (first + last)/2;
		}
		return middle;	
	}

	
	/* 
	 * Moving average smoother, replace data value with average on neighbors
	 * @param tsData time series
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one)
	 * @param kSmoother range of neighbors
	 * @return resultTsData transformed time series
	 */
	public static Hashtable<Integer, Double> movingAverageSmoother(Hashtable<Integer, Double> tsData, int timeStart, int timeEnd, int kSmoother) {
		Hashtable<Integer, Double> resultTsData = new Hashtable<Integer, Double>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		int startIndex = binarySearch(keys,timeStart);
		int endIndex = binarySearch(keys, timeEnd);
		for(int i = startIndex; i <= endIndex; ++i) {
			if (i-kSmoother >= startIndex && i+kSmoother <=endIndex) {
				double tempAverage = mean(tsData, keys.get(i-kSmoother), keys.get(i+kSmoother));
				resultTsData.put(i, tempAverage);
			}
			else if (i-kSmoother < startIndex && i+kSmoother <= endIndex) {
				double tempAverage = mean(tsData, timeStart, keys.get(i+kSmoother));
				resultTsData.put(i, tempAverage);
			}
			else if (i-kSmoother >= startIndex && i+kSmoother > endIndex) {
				double tempAverage = mean(tsData, keys.get(i-kSmoother), timeEnd);
				resultTsData.put(i, tempAverage);
			}
			else {
				double tempAverage = mean(tsData, timeStart, timeEnd);
				resultTsData.put(i, tempAverage);
			}
		}
		return resultTsData;
	}


	/*
	 * Piecewise aggregate approximation of time series
	 * @param tsData time series
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one)
	 * @param dimensions number of dimensions to be used in new time series
	 * @return PAA PAA representation of time series
	 */
	public static Hashtable<Integer, Double> piecewiseAggregateApproximation(Hashtable<Integer, Double> tsData, int timeStart, int timeEnd, int dimensions) {
		Hashtable<Integer, Double> PAA = new Hashtable<Integer, Double>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		int startIndex = binarySearch(keys,timeStart);
		int endIndex = binarySearch(keys, timeEnd);
		int length = endIndex - startIndex + 1;
		for (int i = 1; i <= dimensions; ++i) {
			double temp = 0;
			int start = (length/dimensions)*(i-1);
			int end = (length/dimensions)*i;
			for (int j = start; j < end; ++j) {
				temp += tsData.get(keys.get(j));
			}
			temp *= dimensions;
			temp /= length;
			PAA.put(i, temp);
		}
		return PAA;
	}
	
	
	/*
	 * Symbolic Aggregate Approximation (SAX) of time series, using integers 0,1.. as 
	 * symbols - could further be transformed to binary if required
	 * @param tsData time series
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one)
	 * @param dimensions number of dimensions to be used in new time series
	 * @param cardinality number of breakpoints of the SAX transformation
	 * @return SAX transformation of time series
	 */
	public static Hashtable<Integer, Integer> symbolicAggregateApproximation(Hashtable<Integer, Double> tsData, int timeStart, int timeEnd, int dimensions, int cardinality) {
		// list with start times for each symbol;
		// symbol is represented by index in list;
		ArrayList<Double> mapSymbolTime = new ArrayList<Double>();
		Hashtable<Integer, Double> PAA = piecewiseAggregateApproximation(tsData, timeStart, timeEnd, dimensions);
		
		ArrayList<Integer> keys = new ArrayList<Integer>(PAA.keySet());
		Collections.sort(keys);
		System.out.println(keys);
		
		Double timeRange = range(PAA, keys.get(0), keys.get(keys.size()-1));
		Double intervalDuration = timeRange/cardinality;
				
		for (int i = 0; i < cardinality; ++i) {
			Double intervalStart = keys.get(0) + intervalDuration*i;
			mapSymbolTime.add(intervalStart);
		}
		
		// write new time series symbol values
		Hashtable<Integer, Integer> SAX = new Hashtable<Integer, Integer>();
		for (int i = 0; i < keys.size(); ++i) {
			int position = binarySearch(mapSymbolTime, PAA.get(keys.get(i)));
			SAX.put(i, position);
		}
		return SAX;		
	}
	
	
	/*
	 * DFT of time series
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one)
	 * @return DFTdata computed fourier transform
	 */
	public static Hashtable<Integer, Complex> DFT(Hashtable<Integer, Double> tsData, int timeStart, int timeEnd) {
		Hashtable<Integer, Complex> DFTdata = new Hashtable<Integer, Complex>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
			
		for (int i:keys) {
			double sumReal = 0;
			double sumImag = 0;
			for (int j:keys) {
				double angle = (2*Math.PI*i*j)/(keys.size());
				sumReal = tsData.get(keys.get(i)) * Math.cos(angle);
				sumImag = tsData.get(keys.get(i)) * Math.sin(angle);
			}
			DFTdata.put(i, new Complex(sumReal, sumImag));
		}
		return DFTdata;
	}
}