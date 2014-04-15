package past;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;


public class Transformations {
	
	/*
	 * implements different functions and transformations of the time series
	 */
	private static Double epsilon = 0.00000001; // assume Double as equal
	private static ArrayList<Double> times = new ArrayList<Double>();
	private static ArrayList<Double> data = new ArrayList<Double>();
	
	private static ArrayList<Double> resultsTime = new ArrayList<Double>();
//	private static ArrayList<Double> resultsData = new ArrayList<Double>();
//	private static ArrayList<Complex> resultsDFT = new ArrayList<Complex>();
	
	
	/*
	 * Initialize ArrayLists with data from files; could be split if we 
	 * need to read multiple files with data
	 * @param pathTime path to the file with times
	 * @param pathValues path to the file with data
	 */	
	private static void initTimeSeries(String pathTime, String pathValues) {
		try {
			BufferedReader brT = new BufferedReader(new FileReader(pathTime));
			BufferedReader brV = new BufferedReader(new FileReader(pathValues));
			
			String lineT, lineV;
			while ((lineT = brT.readLine()) != null && (lineV = brV.readLine()) != null) {
				Double tempTime = Double.parseDouble(lineT);
				Double tempVal = Double.parseDouble(lineV);
				
				times.add(tempTime);
				data.add(tempVal);
			}
			brT.close();
			brV.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/*
	 * If only window frame is needed, use binary search to find
	 * position of where the first time occurs
	 * @param timeStart first time of the currently needed time series
	 * @return middle position for the required time series start
	 */
	private static int binarySearch(Double timeStart, ArrayList<Double> dataForBinary) {
		int first = 0;
		int last = dataForBinary.size();
		int middle = (first + last)/2;
		
		while (first <= last) {
			if (dataForBinary.get(middle) < timeStart) {
				first = middle + 1;
			}
			else if (dataForBinary.get(middle) == timeStart) {
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
	 * Power transformation of time series: square root (on complete data set 
	 * or partial window frame with start and end time given)
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return newData transformed time series
	 */
	private static ArrayList<Double> sqrtTransform(Double timeStart, Double timeEnd) {
		ArrayList<Double> newData = new ArrayList<Double>();
		int startIndex = binarySearch(timeStart, data);
		for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
			resultsTime.add(times.get(i));
			newData.add(Math.sqrt(data.get(i)));
		}
		return newData;
	}

	
	/*
	 * Power transformation of time series: logarithm (on complete data set 
	 * or partial window frame with start and end time given)
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return newData transformed time series
	 */
	private static ArrayList<Double> logTransform(Double timeStart, Double timeEnd) {
		ArrayList<Double> newData = new ArrayList<Double>();
		int startIndex = binarySearch(timeStart, data);
		for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
			resultsTime.add(times.get(i));
			newData.add(Math.log(data.get(i)));
		}
		return newData;
	}
	
	
	/*
	 * Compute average of time series (on complete data set 
	 * or partial window frame with start and end time given)
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return avg average of time series
	 */
	private static Double mean(Double timeStart, Double timeEnd) {
		Double avg = .0;
		int count = 0;
		int startIndex = binarySearch(timeStart, data);
		for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
			avg += data.get(i);
			count ++;
		}
		avg /= count;
		return avg;
	}

	
	/*
	 * Compute the average on range intervals and subtract the average from original values
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @param rangeInterval size of the interval on which the average is computed
	 * @return newData transformed time series
	 */
	private static ArrayList<Double> subtractMean(Double timeStart, Double timeEnd, int rangeInterval) {
		ArrayList<Double> newData = new ArrayList<Double>();
		int startIndex = binarySearch(timeStart, data);
		int endIndex = binarySearch(timeEnd, data);
		int i = startIndex;
		while(i + rangeInterval < endIndex) {
			double tempAverage = mean(times.get(i), times.get(i+rangeInterval));
			for (int j = i; j < rangeInterval; ++j) {
				newData.add(data.get(j) - tempAverage);
			}
			i = i + rangeInterval;
		}
		if (i > endIndex) {
			// do the same for the last interval
			double tempAverage = mean(times.get(i-rangeInterval+1), timeEnd);
			for (int j = i-rangeInterval+1; j < timeEnd; ++j) {
				newData.add(data.get(i-rangeInterval+1) - tempAverage);
			}
		}
		return newData;
	}
	
	
	/*
	 * Range of the time series: highest value - lowest value
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @param dataRange time series data on which to compute the range [used for SAX]
	 * @return range time series range on interval
	 */
	private static Double range(Double timeStart, Double timeEnd, ArrayList<Double> dataForRange) {
		int startIndex = binarySearch(timeStart, data);
		ArrayList<Double> sortedData = new ArrayList<Double>();
		for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
			sortedData.add(dataForRange.get(i));
		}
		Collections.sort(sortedData);
		Double range = sortedData.get(sortedData.size()-1) - sortedData.get(0);
		return range;
	}
	
	
	/*
	 * Compute mode of time series (on complete data set 
	 * or partial window frame with start and end time given)
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 * @return maxVal mode of the time series
	 */
	private static Double mode(Double timeStart, Double timeEnd) {
		int startIndex = binarySearch(timeStart, data);
		ArrayList<Double> sortedData = new ArrayList<Double>();
		for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
			sortedData.add(data.get(i));
		}
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
		}
		return maxVal;
	}	

	
	/* 
	 * Moving average smoother, replace data value with average on neighbors
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one)
	 * @param kSmoother range of neighbors
	 * @return newData transformed time series
	 */
	private static ArrayList<Double> movingAverageSmoother(Double timeStart, Double timeEnd, int kSmoother) {
		ArrayList<Double> newData = new ArrayList<Double>();
		int startIndex = binarySearch(timeStart, data);
		int endIndex = binarySearch(timeEnd, data);
		for (int i = startIndex; i <= endIndex; ++i) {
			if (i - kSmoother >= startIndex && i + kSmoother <= endIndex) {
				double tempAverage = mean(times.get(i-kSmoother), times.get(i+kSmoother));
				newData.add(tempAverage);
			}
			else {
				if (i - kSmoother < startIndex && i + kSmoother <= endIndex) {
					double tempAverage = mean(timeStart, times.get(i+kSmoother));
					newData.add(tempAverage);
				}
				else {
					if (i - kSmoother >= startIndex && i + kSmoother > endIndex) {
						double tempAverage = mean(times.get(i-kSmoother), timeEnd);
						newData.add(tempAverage);
					}
					else {
						double tempAverage = mean(timeStart, timeEnd);
						newData.add(tempAverage);
					}
				}
			}		
		}
		return newData;
	}

	
	/*
	 * DFT of time series
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one)
	 * @return DFTdata computed fourier transform
	 */
	private static ArrayList<Complex> DFT(Double timeStart, Double timeEnd) {
		ArrayList<Complex> DFTdata = new ArrayList<Complex>();
		for (int i = 0; i < data.size(); ++i) {
			double sumReal = 0;
			double sumImag = 0;
			for (int j = 0; j < data.size(); ++j) {
				double angle = (2*Math.PI*i*j)/(data.size());
				sumReal = data.get(i) * Math.cos(angle);
				sumImag = data.get(i) * Math.sin(angle);
			}
			DFTdata.add(new Complex(sumReal, sumImag));
		}
		return DFTdata;
	}

	
	/*
	 * DFT uses a different write to file, as it has to write complex numbers
	 * @param pathNewValues path of the output file
	 * @param DFTdata fourier transformed data
	 */
	private static void writeDFT(String pathNewValues, ArrayList<Double> DFTdata) {
		try {
			FileWriter outValues = new FileWriter(pathNewValues);
			BufferedWriter bV = new BufferedWriter(outValues);
			for (int i = 0; i < DFTdata.size(); ++i) {
				bV.write(DFTdata.get(i).toString());
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/*
	 * Write new values to file
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @param newData transformed time series
	 */
	private static void writeToFile(String pathNewTime, String pathNewValues, ArrayList<Double> newData) {
		try {
			FileWriter outTime = new FileWriter(pathNewTime);
			BufferedWriter bT = new BufferedWriter(outTime);
			FileWriter outValues = new FileWriter(pathNewValues);
			BufferedWriter bV = new BufferedWriter(outValues);

			int i = 0, j = 0;
			while (i < resultsTime.size() && i < newData.size()) {
				bT.write(String.valueOf(resultsTime.get(i)));
				bV.write(String.valueOf(newData.get(i)));
				i++;
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/*
	 * Shifting time series (window) with coefficient
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @param coeff coefficient to shift with
	 * @return newData shifted time series
	 */
	private static ArrayList<Double> shift(Double timeStart, Double timeEnd, int coeff) {
		ArrayList<Double> newData = new ArrayList<Double>();
		int startIndex = binarySearch(timeStart, data);
		for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
			resultsTime.add(times.get(i));
			newData.add(data.get(i) + coeff);
		}
		return newData;
	}

	
	/*
	 * Scaling time series (window) with coefficient
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @param coeff coefficient to scale with
	 * @return newData scaled time series
	 */
	private static ArrayList<Double> scale(Double timeStart, Double timeEnd, int coeff) {
		ArrayList<Double> newData = new ArrayList<Double>();
		int startIndex = binarySearch(timeStart, data);
		for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
			resultsTime.add(times.get(i));
			newData.add(data.get(i)*coeff);
		}
		return newData;
	}
	
	
	/*
	 * Standard deviation of the time series
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @return STD standard deviation for time series interval
	 */
	private static Double stdDeviation(Double timeStart, Double timeEnd) {
		int startIndex = binarySearch(timeStart, data);
		double avg = mean(timeStart, timeEnd);
		double STD = 0;
		int count = 0;
		for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
			STD += (times.get(i) - avg)*(times.get(i) - avg);
			count ++;
		}
		STD = Math.sqrt(STD/count);
		return STD;
	}
	
	
	/*
	 * Normalization of time series
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @return newData normalized time series
	 */
	private static ArrayList<Double> normalize(Double timeStart, Double timeEnd) {
		ArrayList<Double> newData = new ArrayList<Double>();
		int startIndex = binarySearch(timeStart, data);
		double avg = mean(timeStart, timeEnd);
		double std = stdDeviation(timeStart, timeEnd);
		for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
			newData.add((data.get(i)-avg)/std);
		}
		return newData;
	}
	
	
	/*
	 * Piecewise aggregate approximation of time series
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @param dimensions number of dimensions to be used in new time series
	 * @return PAA PAA representation of time series
	 */
	private static ArrayList<Double> piecewiseAggregateApproximation(Double timeStart, Double timeEnd, int dimensions) {
		ArrayList<Double> PAA = new ArrayList<Double>();
		int startIndex = binarySearch(timeStart, data);
		int endIndex = binarySearch(timeEnd, data);
		int length = endIndex - startIndex;
		for (int i = 0; i < dimensions; ++i) {
			double temp = 0;
			int start = (length/dimensions)*(i-1)+1;
			int end = (length/dimensions)*i;
			for (int j = start; j < end; ++j) {
				temp += data.get(j);
			}
			temp *= (dimensions/length);
			PAA.add(temp);
			temp = 0;
		}
		return PAA;
	}
	
	
	/*
	 * Symbolic Aggregate Approximation (SAX) of time series, using integers 0,1.. as 
	 * symbols - could further be transformed to binary if required
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @param dimensions number of dimensions to be used in new time series
	 * @param cardinality number of breakpoints of the SAX transformation
	 * @return SAX transformation of time series
	 */
	private static ArrayList<Integer> symbolicAggregateApproximation(Double timeStart, Double timeEnd, int dimensions, int cardinality) {
		// list with start times for each symbol;
		// symbol is represented by index in list;
		ArrayList<Double> mapSymbolTime = new ArrayList<Double>();
		ArrayList<Double> PAA = piecewiseAggregateApproximation(timeStart, timeEnd, dimensions);
		Double timeRange = range(PAA.get(0), PAA.get(PAA.size()), PAA);
		Double intervalDuration = timeRange/cardinality;
				
		for (int i = 0; i < cardinality; ++i) {
			Double intervalStart = PAA.get(0) + intervalDuration*i;
			mapSymbolTime.add(intervalStart);
		}
		
		// write new time series symbol values
		ArrayList<Integer> SAX = new ArrayList<Integer>();
		for (int i = 0; i < PAA.size(); ++i) {
			int position = binarySearch(PAA.get(i), mapSymbolTime);
			SAX.add(position);
		}
		return SAX;		
	}
}