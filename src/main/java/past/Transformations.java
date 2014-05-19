package past;

import java.awt.Point;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
//import past.storage.DBType;


public class Transformations {
	/*
	 * implements different functions and transformations of the time series
	 */
	public static Double epsilon = 0.00000001; // assume Double as equal

	
	/*
	 * Power transformation of time series: square root (on complete data set 
	 * or partial window frame with start and end time given)
	 * @param ts time series
	 * @param attribute
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return resultTsData transformed time series
	 */
	public static Hashtable<Integer, Object> sqrtTransform(Timeseries ts, String attribute, int timeStart, int timeEnd) {
		Hashtable<Integer, Object> tsData = ts.getTimeseries().get(attribute);
		
		Hashtable<Integer, Object> resultTsData = new Hashtable<Integer, Object>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				Double temp = Math.sqrt((Double)tsData.get(i));
				resultTsData.put(i, temp);
			}
		}
		return resultTsData;
	}

	
	/*
	 * Power transformation of time series: logarithm (on complete data set 
	 * or partial window frame with start and end time given)
	 * @param ts time series
	 * @param attribute
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return resultTsData transformed time series
	 */
	public static Hashtable<Integer, Object> logTransform(Timeseries ts, String attribute, int timeStart, int timeEnd) {
		Hashtable<Integer, Object> tsData = ts.getTimeseries().get(attribute);
		
		Hashtable<Integer, Object> resultTsData = new Hashtable<Integer, Object>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				Double temp = Math.log((Double)tsData.get(i));
				resultTsData.put(i, temp);
			}
		}
		return resultTsData;
	}
	
	
	/*
	 * Compute average of time series (on complete data set 
	 * or partial window frame with start and end time given)
	 * @param ts time series
	 * @param attribute
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return avg average of time series
	 */
	public static Double mean(Timeseries ts, String attribute, int timeStart, int timeEnd) {
		Hashtable<Integer, Object> tsData = ts.getTimeseries().get(attribute);
		
		Double avg = .0;
		int count = 0;
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				avg += (Double)tsData.get(i);
				count ++;
			}
		}
		avg /= count;
		return avg;
	}

	
	/*
	 * Compute the average on interval and subtract the average from original values
	 * @param ts time series
	 * @param attribute
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return resultTsData transformed time series
	 */
	public static Hashtable<Integer, Object> subtractMean(Timeseries ts, String attribute, int timeStart, int timeEnd) {
		Hashtable<Integer, Object> tsData = ts.getTimeseries().get(attribute);
		
		Hashtable<Integer, Object> resultTsData = new Hashtable<Integer, Object>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		double average = mean(ts, attribute, timeStart, timeEnd);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				resultTsData.put(i, (Double)tsData.get(i)-average);
			}
		}
		return resultTsData;
	}
	
	
	/*
	 * Range of the time series: highest value - lowest value
	 * @param ts time series
	 * @param attribute
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return range time series range on interval
	 */
	public static Double range(Timeseries ts, String attribute, int timeStart, int timeEnd) {
		Hashtable<Integer, Object> tsData = ts.getTimeseries().get(attribute);	
		
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Double min = (Double)tsData.get(timeStart);
		Double max = (Double)tsData.get(timeStart);
		
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				if ((Double)tsData.get(i) < min) min = (Double)tsData.get(i);
				if ((Double)tsData.get(i) > max) max = (Double)tsData.get(i);
			}
		}
		
		Double range = max - min;
		return range;
	}
	
	public static Double range(Hashtable<Integer, Object> tsData, int timeStart, int timeEnd) {
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Double min = (Double)tsData.get(timeStart);
		Double max = (Double)tsData.get(timeStart);
		
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				if ((Double)tsData.get(i) < min) min = (Double)tsData.get(i);
				if ((Double)tsData.get(i) > max) max = (Double)tsData.get(i);
			}
		}
		
		Double range = max - min;
		return range;
	}

	/*
	 * Helper function to extract only partial values from the time series
	 * @param ts time series
	 * @param attribute
	 * @param timeStart start time of frame
	 * @param timeEnd end time of frame
	 * @return resultFrame values extracted from time series
	 */
	public static ArrayList<Double> extractFrame(Timeseries ts, String attribute, int timeStart, int timeEnd) {
		Hashtable<Integer, Object> tsData = ts.getTimeseries().get(attribute);
		
		ArrayList<Double> resultFrame = new ArrayList<Double>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i > timeStart && i < timeEnd) {
				resultFrame.add((Double)tsData.get(i));
			}
		}
		return resultFrame;
	}

	
	/*
	 * Compute mode of time series (on complete data set 
	 * or partial window frame with start and end time given)
	 * @param ts time series
	 * @param attribute
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return maxVal mode of the time series
	 */
	public static Double mode(Timeseries ts, String attribute, int timeStart, int timeEnd) {
		Hashtable<Integer, Object> tsData = ts.getTimeseries().get(attribute);

		ArrayList<Double> sortedData = extractFrame(ts, attribute, timeStart, timeEnd);
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
	 * @param ts time series
	 * @param attribute
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @param coeff coefficient to shift with
	 * @return resultTsData shifted time series
	 */
	public static Hashtable<Integer, Object> shift(Timeseries ts, String attribute, int timeStart, int timeEnd, int coeff) {
		Hashtable<Integer, Object> tsData = ts.getTimeseries().get(attribute);

		Hashtable<Integer, Object> resultTsData = new Hashtable<Integer, Object>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				resultTsData.put(i, (Double)tsData.get(i)+coeff);
			}
		}
		return resultTsData;
	}

	
	/*
	 * Scaling time series (window) with coefficient
	 * @param ts time series
	 * @param attribute
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @param coeff coefficient to scale with
	 * @return resultTsData scaled time series
	 */
	public static Hashtable<Integer, Object> scale(Timeseries ts, String attribute, int timeStart, int timeEnd, int coeff) {
		Hashtable<Integer, Object> tsData = ts.getTimeseries().get(attribute);

		Hashtable<Integer, Object> resultTsData = new Hashtable<Integer, Object>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				resultTsData.put(i, (Double)tsData.get(i)*coeff);
			}
		}
		return resultTsData;
	}
	
	
	/*
	 * Standard deviation of the time series
	 * @param ts time series
	 * @param attribute
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @return STD standard deviation for time series interval
	 */
	public static Double stdDeviation(Timeseries ts, String attribute, int timeStart, int timeEnd) {
		Hashtable<Integer, Object> tsData = ts.getTimeseries().get(attribute);

		double avg = mean(ts, attribute, timeStart, timeEnd);
		double STD = 0;
		int count = 0;
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				STD += (((Double)tsData.get(i)).doubleValue()-avg)*(((Double)tsData.get(i)).doubleValue()-avg);
				count ++;
			}
		}
		STD = Math.sqrt(STD/count);
		return STD;
	}
	
	
	/*
	 * Normalization of time series
	 * @param ts time series
	 * @param attribute
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @return resultTsData normalized time series
	 */
	public static Hashtable<Integer, Object> normalize(Timeseries ts, String attribute, int timeStart, int timeEnd) {
		Hashtable<Integer, Object> tsData = ts.getTimeseries().get(attribute);

		double avg = mean(ts, attribute, timeStart, timeEnd);
		double std = stdDeviation(ts, attribute, timeStart, timeEnd);
		Hashtable<Integer, Object> resultTsData = new Hashtable<Integer, Object>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		for (int i:keys) {
			if (i >= timeStart && i <= timeEnd) {
				resultTsData.put(i, (((Double)tsData.get(i)).doubleValue()-avg)/std);
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

	public static int binarySearch(ArrayList<Object> searchData, Double timeStart) {
		int first = 0;
		int last = searchData.size() - 1;
		int middle = (first + last)/2;
		
		while (first <= last) {
			if ((Double)searchData.get(middle) < timeStart) {
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
	 * @param ts time series
	 * @param attribute
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one)
	 * @param kSmoother range of neighbors
	 * @return resultTsData transformed time series
	 */
	public static Hashtable<Integer, Object> movingAverageSmoother(Timeseries ts, String attribute, int timeStart, int timeEnd, int kSmoother) {
		Hashtable<Integer, Object> tsData = ts.getTimeseries().get(attribute);

		Hashtable<Integer, Object> resultTsData = new Hashtable<Integer, Object>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
		int startIndex = binarySearch(keys,timeStart);
		int endIndex = binarySearch(keys, timeEnd);
		for(int i = startIndex; i <= endIndex; ++i) {
			if (i-kSmoother >= startIndex && i+kSmoother <=endIndex) {
				double tempAverage = mean(ts, attribute, keys.get(i-kSmoother), keys.get(i+kSmoother));
				resultTsData.put(i, tempAverage);
			}
			else if (i-kSmoother < startIndex && i+kSmoother <= endIndex) {
				double tempAverage = mean(ts, attribute, timeStart, keys.get(i+kSmoother));
				resultTsData.put(i, tempAverage);
			}
			else if (i-kSmoother >= startIndex && i+kSmoother > endIndex) {
				double tempAverage = mean(ts, attribute, keys.get(i-kSmoother), timeEnd);
				resultTsData.put(i, tempAverage);
			}
			else {
				double tempAverage = mean(ts, attribute, timeStart, timeEnd);
				resultTsData.put(i, tempAverage);
			}
		}
		return resultTsData;
	}


	/*
	 * Piecewise aggregate approximation of time series
	 * @param ts time series
	 * @param attribute
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one)
	 * @param dimensions number of dimensions to be used in new time series
	 * @return PAA PAA representation of time series
	 */
	public static Hashtable<Integer, Object> piecewiseAggregateApproximation(Timeseries ts, String attribute, int timeStart, int timeEnd, int dimensions) {
		Hashtable<Integer, Object> tsData = ts.getTimeseries().get(attribute);

		Hashtable<Integer, Object> PAA = new Hashtable<Integer, Object>();
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
				temp += ((Double)tsData.get(keys.get(j))).doubleValue();
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
	 * @param ts time series
	 * @param attribute
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one)
	 * @param dimensions number of dimensions to be used in new time series
	 * @param cardinality number of breakpoints of the SAX transformation
	 * @return SAX transformation of time series
	 */
	//TODO: store cardinality in point
	public static Hashtable<Integer, Point> symbolicAggregateApproximation(Timeseries ts, String attribute, int timeStart, int timeEnd, int dimensions, int cardinality) {
		Hashtable<Integer, Object> tsData = ts.getTimeseries().get(attribute);

		// list with start times for each symbol;
		// symbol is represented by index in list;
		ArrayList<Object> mapSymbolTime = new ArrayList<Object>();
		Hashtable<Integer, Object> PAA = piecewiseAggregateApproximation(ts, attribute, timeStart, timeEnd, dimensions);
		
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
		Hashtable<Integer, Point> SAX = new Hashtable<Integer, Point>();
		for (int i = 0; i < keys.size(); ++i) {
			int position = binarySearch(mapSymbolTime, (Double)PAA.get(keys.get(i)));
			SAX.put(i, new Point(position, cardinality));
		}
		return SAX;		
	}
	
	
	/*
	 * DFT of time series
	 * @param ts time series
	 * @param attribute
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one)
	 * @return DFTdata computed fourier transform
	 */
	public static Hashtable<Integer, Complex> DFT(Timeseries ts, String attribute, int timeStart, int timeEnd) {
		Hashtable<Integer, Object> tsData = ts.getTimeseries().get(attribute);

		Hashtable<Integer, Complex> DFTdata = new Hashtable<Integer, Complex>();
		ArrayList<Integer> keys = new ArrayList<Integer>(tsData.keySet());
		Collections.sort(keys);
			
		for (int i:keys) {
			double sumReal = 0;
			double sumImag = 0;
			for (int j:keys) {
				double angle = (2*Math.PI*i*j)/(keys.size());
				sumReal = (Double)tsData.get(keys.get(i)) * Math.cos(angle);
				sumImag = (Double)tsData.get(keys.get(i)) * Math.sin(angle);
			}
			DFTdata.put(i, new Complex(sumReal, sumImag));
		}
		return DFTdata;
	}
	
	
	/*
	 * Dynamic time warping - similarity between two time series
	 * @param ts1 first time series to compare, with
	 * @param attr1 its attribute file
	 * @param ts2 second time series with
	 * @param attr2 its attribute file
	 * @return similarity value
	 */
	public static double DTWDistance(Timeseries ts1, String attr1, Timeseries ts2, String attr2) {
		Hashtable<Integer, Object> TS1 = ts1.getTimeseries().get(attr1);
		Hashtable<Integer, Object> TS2 = ts2.getTimeseries().get(attr2);

		Hashtable <Point, Double> DTW = new Hashtable <Point, Double>();
		
		DTW.put(new Point(0,0), .0);
		for (int i = 1; i <= TS1.size(); ++i) {
			DTW.put(new Point(i,0), Double.MAX_VALUE);
		}
		for (int i = 1; i <= TS2.size(); ++i) {
			DTW.put(new Point(0,i), Double.MAX_VALUE);
		}
		
		// algorithm
		for (int i = 1; i <= TS1.size(); ++i) {
			for (int j = 1; j <= TS2.size(); ++j) {
				// choose other cost?
				double cost = Math.abs((Double)TS1.get(i-1) - (Double)TS2.get(j-1));
				double min1 = Math.min(DTW.get(new Point(i-1, j)), DTW.get(new Point(i, j-1)));
				double min2 = Math.min(min1, DTW.get(new Point(i-1, j-1)));
				double value = cost + min2;
				DTW.put(new Point(i,j), value);
			}
		}
		return DTW.get(new Point(TS1.size(), TS2.size()));
	}
	
}
