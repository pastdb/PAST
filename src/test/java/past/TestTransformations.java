package past;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;

public class TestTransformations {
	
	private static Hashtable<Integer, Double> series = new Hashtable <Integer, Double>();
	
	public static void initTimeSeries() {
		series.put(0, 1.2);
		series.put(1, 1.3);
		series.put(2, 1.2);
		series.put(3, 1.8);
		series.put(4, 2.5);
		series.put(5, 4.7);
		series.put(6, 3.2);
		series.put(7, 3.2);
		series.put(8, 3.2);
		series.put(9, 5.9);
		series.put(10, 7.3);
		series.put(11, 7.1);
		series.put(12, 10.0);
		series.put(13, 12.1);
		series.put(14, 9.2);
		series.put(15, 9.7);
	}
	
	public static void main(String args[]) {
		initTimeSeries();
		int tstart = 0;
		int tend = 15;

		Hashtable<Integer, Double> result = new Hashtable<Integer, Double>();
		Hashtable<Integer, Integer> resultSAX = new Hashtable<Integer, Integer>();

		double resultV = 0.0;
		
		// ADD transformation here
		//result = Transformations.sqrtTransform(series, tstart, tend);				
		//result: 1.0954451150103321 1.140175425099138 1.0954451150103321 1.3416407864998738 1.5811388300841898 2.16794833886788 1.7888543819998317 1.7888543819998317 1.7888543819998317 2.4289915602982237 2.701851217221259 2.6645825188948455 3.1622776601683795 3.478505426185217 3.03315017762062 3.1144823004794873 0.0
		
		//result = Transformations.sqrtTransform(series, tstart, tend);				
		//result: 1.0954451150103321 1.140175425099138 1.0954451150103321 1.3416407864998738 1.5811388300841898 2.16794833886788 1.7888543819998317 1.7888543819998317 1.7888543819998317 2.4289915602982237 2.701851217221259 2.6645825188948455 3.1622776601683795 3.478505426185217 3.03315017762062 3.1144823004794873 0.0
		
		//resultV = Transformations.mean(series, tstart, tend);						
		//result: 5.225
		
		//result = Transformations.subtractMean(series, tstart, tend);				
		//result: -4.0249999999999995 -3.925 -4.0249999999999995 -3.425 -2.7249999999999996 -0.5249999999999995 -2.0249999999999995 -2.0249999999999995 -2.0249999999999995 0.6750000000000007 2.075 1.875 4.775 6.875 3.9749999999999996 4.475 0.0
		
		//resultV = Transformations.range(series, tstart, tend);					
		//result: 10.9
		
		//result = Transformations.movingAverageSmoother(series, tstart, tend, 3);	
		//result: 1.375 1.6 2.1166666666666667 2.2714285714285714 2.557142857142857 2.8285714285714283 3.5 4.2857142857142865 4.942857142857143 5.7 6.971428571428572 7.828571428571428 8.757142857142856 9.233333333333334 9.620000000000001 10.25 0.0
		
		//result = Transformations.shift(series, tstart, tend, 2);					
		//result: 3.2 3.3 3.2 3.8 4.5 6.7 5.2 5.2 5.2 7.9 9.3 9.1 12.0 14.1 11.2 11.7 0.0
		
		//result = Transformations.scale(series, tstart, tend, 10);					
		//result: 12.0 13.0 12.0 18.0 25.0 47.0 32.0 32.0 32.0 59.0 73.0 71.0 100.0 121.0 92.0 97.0 0.0
		
		//resultV = Transformations.stdDeviation(series, tstart, tend);				
		//result: 3.4794934976228937
		
		//result = Transformations.normalize(series, tstart, tend);					
		//result: -1.156777560512696 -1.1280377453446788 -1.156777560512696 -0.9843386695045924 -0.7831599633284713 -0.15088402963209066 -0.5819812571523502 -0.5819812571523502 -0.5819812571523502 0.19399375238411695 0.596351164736359 0.5388715344003243 1.372326174272826 1.9758622928011893 1.1424076529286875 1.286106728768774
		
		//result = Transformations.piecewiseAggregateApproximation(series, tstart, tend, 4); 		
		//result: 1.375 3.4000000000000004 5.875 10.25 
		
		//resultSAX = Transformations.symbolicAggregateApproximation(series, tstart, tend, 4, 2); 	
		//result: 0 0 1 1 
		
		//resultSAX = Transformations.symbolicAggregateApproximation(series, tstart, tend, 4, 6);	
		//result: 0 1 3 5 
	
		//resultV = Transformations.mode(series, tstart, tend);
		//result: 3.2
		
		ArrayList<Integer> keys = new ArrayList<Integer>(resultSAX.keySet());
		Collections.sort(keys);
		System.out.print("result: ");
		for (int i:keys) {
			System.out.print(resultSAX.get(i) + " ");
		}
		System.out.println();
		System.out.println(resultV);	
	}
}
