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
	
	private static Double epsilon = 0.000001; // assume equal
	private static ArrayList<Double> times = new ArrayList<Double>();
	private static ArrayList<Double> data = new ArrayList<Double>();
	
	private static ArrayList<Double> resultsTime = new ArrayList<Double>();
	private static ArrayList<Double> resultsData = new ArrayList<Double>();
	private static ArrayList<Complex> resultsDFT = new ArrayList<Complex>();

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
	
	private static void sqrtTransform(Double timeStart, Double timeEnd, Boolean sw) {
		int startIndex = binarySearch(timeStart);
		if (sw) { //output only window frame
			for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
				resultsTime.add(times.get(i));
				resultsData.add(Math.sqrt(data.get(i)));
			}
		}
		else {
			resultsTime = times;
			for (int i = 0; i < times.size(); ++i) {
				if (times.get(i) >= timeStart && times.get(i) <= timeEnd) {
					resultsData.add(Math.sqrt(data.get(i)));
				}
				else {
					resultsData.add(data.get(i));
				}
			}
		}
	}
	
	private static void logTransform(Double timeStart, Double timeEnd, Boolean sw) {
		int startIndex = binarySearch(timeStart);
		if (sw) { // only for window frame
			for (int i = startIndex; times.get(i) <= timeEnd; ++i) {
				resultsTime.add(times.get(i));
				resultsData.add(Math.log(data.get(i)));
			}
		}
		else {
			resultsTime = times;
			for (int i = 0; i < times.size(); ++i) {
				if (times.get(i) >= timeStart && times.get(i) <= timeEnd) {
					resultsData.add(Math.log(data.get(i)));
				}
				else {
					resultsData.add(data.get(i));
				}
			}
		}
	}
	
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
}