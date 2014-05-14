package past;

import java.awt.Point;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;

import past.Transformations;


public class APCluster {
	private static Transformations tr;
	private static int NB;
	private static int iter;
	private static double lambda;
	Hashtable <Timeseries, String> TS = new Hashtable<Timeseries, String>();
	Hashtable <Point, Double> similarity = new Hashtable <Point, Double>();
	Hashtable <Point, Double> responsibility = new Hashtable <Point, Double>();
	Hashtable <Point, Double> availability = new Hashtable <Point, Double>();
	Hashtable <Point, Double> exemplar = new Hashtable <Point, Double>();
	Hashtable <Integer, Integer> idx = new Hashtable <Integer, Integer>();

	public APCluster(Hashtable<Timeseries, String> ts, int it, double lmb) {
		TS = ts;
		NB = TS.size();
		iter = it;
		lambda = lmb;
	}
	
	public void computeSimilarities() {
		ArrayList<Timeseries> keys = new ArrayList<Timeseries>(TS.keySet());
		ArrayList<String> values = new ArrayList<String>(TS.values());
		ArrayList<Double> tempVals = new ArrayList<Double>();
		for (int i = 0; i < NB; i++) {
			for (int j = i+1; j < NB; j++) {
				if (i != j) {
					double temp = - tr.DTWDistance(keys.get(i), values.get(i), keys.get(j), values.get(j));
					similarity.put(new Point(i, j), temp);
					similarity.put(new Point(j, i), temp);
					tempVals.add(temp);
				}
			}
		}
		
		// compute preferences for all data points(median)
		Collections.sort(tempVals);
		double median = 0;
		int size = NB*(NB-1)/2;
		if (size % 2 == 0) {
			median = tempVals.get(size/2) + tempVals.get(size/2 - 1)/2;
		}
		else {
			median = tempVals.get(size/2);
		}
		
		for (int i = 0; i < NB; i++) {
			similarity.put(new Point(i, i), median);
		}
		
	}
	
	
	public void init() {
		for (int i = 0; i < NB; i++) {
			for (int j = 0; j < NB; j++) {
				similarity.put(new Point(i, j), .0);
				responsibility.put(new Point(i, j), .0);
				availability.put(new Point(i, j), .0);
				exemplar.put(new Point(i, j), .0);
				idx.put(i, 0);
			}
		}
		computeSimilarities();
	}
	
	
	public void cluster() {
		init();
		
		for (int n = 0; n < iter; n++) {
			// update responsibility matrix
			for (int i = 0; i < NB; i++) {
				for (int k = 0; k < NB; k++) {
					double max = Double.MIN_VALUE;
					for (int kk = 0; kk < k; kk++) {
						if (similarity.get(new Point(i, kk)) + availability.get(new Point(i, kk)) > max) {
							max = similarity.get(new Point(i, kk)) + availability.get(new Point(i, kk));
						}
					}
					for (int kk = k+1; kk < NB; kk++) {
						if (similarity.get(new Point(i, kk)) + availability.get(new Point(i, kk))> max) {
							max = similarity.get(new Point(i, kk)) + availability.get(new Point(i, kk));
						}
					}
					double value = (1-lambda) * (similarity.get(new Point(i, k))- max) + lambda*responsibility.get(new Point(i, k));
					responsibility.put(new Point(i, k), value);
				}
			}
			
			// update availability matrix
			for (int i = 0; i < NB; i++) {
				for (int k = 0; k < NB; k++) {
					if (i == k) {
						double sum = .0;
						for (int ii = 0; ii < i; ii++) {
							sum += Math.max(.0, responsibility.get(new Point(ii, k)));
						}
						for (int ii = 0; ii < NB; i++) {
							sum += Math.max(.0, responsibility.get(new Point(ii, k)));
						}
						double value = (1-lambda)*sum + lambda*availability.get(new Point(i, k));
						availability.put(new Point(i, k), value);
					}
					else {
						double sum = .0;
						int maxik = Math.max(i, k);
						int minik = Math.min(i, k);
						for (int ii = 0; ii < minik; ii++) {
							sum += Math.max(.0, responsibility.get(new Point(ii, k)));
						}
						for (int ii = minik+1; ii < maxik; ii++) {
							sum += Math.max(.0, responsibility.get(new Point(ii, k)));
						}
						for (int ii = maxik+1; ii < NB; ii++) {
							sum += Math.max(.0, responsibility.get(new Point(ii, k)));
						}
						double value = (1-lambda)*Math.min(.0, responsibility.get(new Point(k, k))+sum) + lambda*availability.get(new Point(i,k));
						availability.put(new Point(i, k), value);
					}
				}
			}
				
		}// nb iterations		
		
		// find the exemplar/ center
		ArrayList<Integer> center = new ArrayList<Integer>();
		for (int i = 0; i < NB; i++) {
			exemplar.put(new Point(i, i), responsibility.get(new Point(i, i)) + availability.get(new Point(i, i)));
			if (exemplar.get(new Point(i, i)) > .0) {
				center.add(i);
			}
		}
		
		// data point assignment, idx(i) is the exemplar for data point i
		for (int i = 0; i < NB; i++) {
			int idxI = 0;
			double maxSim = Double.MIN_VALUE;
			for (int j = 0; j < center.size(); j++) {
				int c = center.get(j);
				if (similarity.get(new Point(i, c)) > maxSim) {
					maxSim = similarity.get(new Point(i, c));
					idxI = c;
				}
			}
			idx.put(i, idxI);
		}
		
		//for (int i = 0; i < NB; i++) {
		//	System.out.println(idx.get(i) + 1);
		//}
	}
}