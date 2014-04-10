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
	private static ArrayList<Double> resultsData = new ArrayList<Double>();
	private static ArrayList<Complex> resultsDFT = new ArrayList<Complex>();
	
	
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
	 */
	private static int binarySearch(Double timeStart) {
		int first = 0;
		int last = data.size();
		int middle = (first + last)/2;
		
		while (first <= last) {
			if (data.get(middle) < timeStart) {
				first = middle + 1;
			}
			else if (data.get(middle) == timeStart) {
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
	 */
	private static void sqrtTransform(Double timeStart, Double timeEnd) {
		int startIndex = binarySearch(timeStart);
		for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
			resultsTime.add(times.get(i));
			resultsData.add(Math.sqrt(data.get(i)));
		}
	}

	
	/*
	 * Power transformation of time series: logarithm (on complete data set 
	 * or partial window frame with start and end time given)
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 */
	private static void logTransform(Double timeStart, Double timeEnd) {
		int startIndex = binarySearch(timeStart);
		for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
			resultsTime.add(times.get(i));
			resultsData.add(Math.log(data.get(i)));
		}
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
		int startIndex = binarySearch(timeStart);
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
	 */
	private static void subtractMean(Double timeStart, Double timeEnd, int rangeInterval) {
		int startIndex = binarySearch(timeStart);
		int endIndex = binarySearch(timeEnd);
		int i = startIndex;
		while(i + rangeInterval < endIndex) {
			double tempAverage = mean(times.get(i), times.get(i+rangeInterval));
			for (int j = i; j < rangeInterval; ++j) {
				resultsData.add(data.get(j) - tempAverage);
			}
			i = i + rangeInterval;
		}
		if (i > endIndex) {
			// do the same for the last interval
			double tempAverage = mean(times.get(i-rangeInterval+1), timeEnd);
			for (int j = i-rangeInterval+1; j < timeEnd; ++j) {
				resultsData.add(data.get(i-rangeInterval+1) - tempAverage);
			}
		}
	}
	
	
	/*
	 * Range of the time series: highest value - lowest value
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one) 
	 */
	private static Double range(Double timeStart, Double timeEnd) {
		int startIndex = binarySearch(timeStart);
		ArrayList<Double> sortedData = new ArrayList<Double>();
		for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
			sortedData.add(data.get(i));
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
		int startIndex = binarySearch(timeStart);
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
	 * Method used to filter given windows of the time series by replacing with 
	 * mode over the window frame
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one)
	 */
	private static void filterByMode(Double timeStart, Double timeEnd) {
		Double mode = mode(timeStart, timeEnd);
		for (int i = 0; i < data.size(); ++i) {
			if (times.get(i) >= timeStart && times.get(i) <= timeEnd) {
				Double newTime = (timeEnd + timeStart)/2.0;
				resultsTime.add(newTime);
				resultsData.add(mode);
				while (times.get(i) >= timeStart && times.get(i) <= timeEnd) {
					i++;
				}
			}
			resultsTime.add(times.get(i));
			resultsData.add(data.get(i));
		}
	}

	
	/* 
	 * Moving average smoother, replace data value with average on neighbors
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one)
	 * @param kSmoother range of neighbors
	 */
	private static void movingAverageSmoother(Double timeStart, Double timeEnd, int kSmoother) {
		int startIndex = binarySearch(timeStart);
		int endIndex = binarySearch(timeEnd);
		for (int i = startIndex; i <= endIndex; ++i) {
			if (i - kSmoother >= startIndex && i + kSmoother <= endIndex) {
				double tempAverage = mean(times.get(i-kSmoother), times.get(i+kSmoother));
				resultsData.add(tempAverage);
			}
			else {
				if (i - kSmoother < startIndex && i + kSmoother <= endIndex) {
					double tempAverage = mean(timeStart, times.get(i+kSmoother));
					resultsData.add(tempAverage);
				}
				else {
					if (i - kSmoother >= startIndex && i + kSmoother > endIndex) {
						double tempAverage = mean(times.get(i-kSmoother), timeEnd);
						resultsData.add(tempAverage);
					}
					else {
						double tempAverage = mean(timeStart, timeEnd);
						resultsData.add(tempAverage);
					}
				}
			}		
		}
	}

	
	/*
	 * DFT of time series
	 * @param timeStart start of the time frame (can be the first one)
	 * @param timeEnd end of the time frame (can be the last one)
	 */
	private static void DFT(Double timeStart, Double timeEnd) {
		for (int i = 0; i < data.size(); ++i) {
			double sumReal = 0;
			double sumImag = 0;
			for (int j = 0; j < data.size(); ++j) {
				double angle = (2*Math.PI*i*j)/(data.size());
				sumReal = data.get(i) * Math.cos(angle);
				sumImag = data.get(i) * Math.sin(angle);
			}
			resultsDFT.add(new Complex(sumReal, sumImag));
		}
	}

	
	/*
	 * DFT uses a different write to file, as it has to write complex numbers
	 * @param pathNewValues path of the output file
	 */
	private static void writeDFT(String pathNewValues) {
		try {
			FileWriter outValues = new FileWriter(pathNewValues);
			BufferedWriter bV = new BufferedWriter(outValues);
			for (int i = 0; i < resultsDFT.size(); ++i) {
				bV.write(resultsDFT.get(i).toString());
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
	 */
	private static void writeToFile(String pathNewTime, String pathNewValues) {
		try {
			FileWriter outTime = new FileWriter(pathNewTime);
			BufferedWriter bT = new BufferedWriter(outTime);
			FileWriter outValues = new FileWriter(pathNewValues);
			BufferedWriter bV = new BufferedWriter(outValues);

			int i = 0, j = 0;
			while (i < resultsTime.size() && i < resultsData.size()) {
				bT.write(String.valueOf(resultsTime.get(i)));
				bV.write(String.valueOf(resultsData.get(i)));
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
	 */
	private static void shift(Double timeStart, Double timeEnd, int coeff) {
		int startIndex = binarySearch(timeStart);
		for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
			resultsTime.add(times.get(i));
			resultsData.add(data.get(i) + coeff);
		}
	}

	
	/*
	 * Scaling time series (window) with coefficient
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 * @param coeff coefficient to scale with
	 */
	private static void scale(Double timeStart, Double timeEnd, int coeff) {
		int startIndex = binarySearch(timeStart);
		for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
			resultsTime.add(times.get(i));
			resultsData.add(data.get(i)*coeff);
		}
	}
	
	
	/*
	 * Standard deviation of the time series
	 * @param pathNewTime path of the output file for time values
	 * @param pathNewValues path of the output file for data values
	 */
	private static Double stdDeviation(Double timeStart, Double timeEnd) {
		int startIndex = binarySearch(timeStart);
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
	 * @return new time series
	 */
	private static void normalize(Double timeStart, Double timeEnd) {
		int startIndex = binarySearch(timeStart);
		double avg = mean(timeStart, timeEnd);
		double std = stdDeviation(timeStart, timeEnd);
		for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
			resultsData.add((data.get(i)-avg)/std);
		}
	}
	
}