package past;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;

import static org.junit.Assert.*;

public class TestTransformations {
	
	private static Hashtable<Integer, Double> series = new Hashtable <Integer, Double>();
	private static int tstart = 0;
	private static int tend = 15;
	
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
	
	@SuppressWarnings("static-access")
	public static void testTransformations() {
		Transformations T = new Transformations();
		
		assertEquals(5.225, T.mean(series, tstart, tend), T.epsilon);
		assertEquals(10.9, T.range(series, tstart, tend), T.epsilon);
		assertEquals(3.4794934976228937, T.stdDeviation(series, tstart, tend), T.epsilon);
		assertEquals(3.2, T.mode(series, tstart, tend), T.epsilon);
		for (int i = 0; i < series.size(); ++i) {
			assertEquals(Math.sqrt(series.get(i)), T.sqrtTransform(series, tstart, tend).get(i), T.epsilon);
		}
		for (int i = 0; i < series.size(); ++i) {
			assertEquals(Math.log(series.get(i)), T.logTransform(series, tstart, tend).get(i), T.epsilon);
		}
		Double average = T.mean(series, tstart, tend);
		for (int i = 0; i < series.size(); ++i) {
			assertEquals(series.get(i)-average, T.subtractMean(series, tstart, tend).get(i), T.epsilon);
		}
		
		int shift = 2;
		for (int i = 0; i < series.size(); ++i) {
			assertEquals(series.get(i)+shift, T.shift(series, tstart, tend, shift).get(i), T.epsilon);
		}
		
		int scale = 10;
		for (int i = 0; i < series.size(); ++i) {
			assertEquals(series.get(i)*scale, T.scale(series, tstart, tend, scale).get(i), T.epsilon);
		}
		Double std = T.stdDeviation(series, tstart, tend);
		for (int i = 0; i < series.size(); ++i) {
			assertEquals((series.get(i)-average)/std, T.normalize(series, tstart, tend).get(i), T.epsilon);
		}
		
	}
	
	public static void main(String args[]) {
		initTimeSeries();
		testTransformations();
		
		
		Hashtable<Integer, Double> result = new Hashtable<Integer, Double>();

		//result = Transformations.movingAverageSmoother(series, tstart, tend, 3);	
		//result: 1.375 1.6 2.1166666666666667 2.2714285714285714 2.557142857142857 2.8285714285714283 3.5 4.2857142857142865 4.942857142857143 5.7 6.971428571428572 7.828571428571428 8.757142857142856 9.233333333333334 9.620000000000001 10.25 0.0
		
		//result = Transformations.piecewiseAggregateApproximation(series, tstart, tend, 4); 		
		//result: 1.375 3.4000000000000004 5.875 10.25 
		
		//resultSAX = Transformations.symbolicAggregateApproximation(series, tstart, tend, 4, 2); 	
		//result: 0 0 1 1 
		
		//resultSAX = Transformations.symbolicAggregateApproximation(series, tstart, tend, 4, 6);	
		//result: 0 1 3 5 
	
		//resultDFT = Transformations.DFT(series, tstart, tend);
		/*
		result: 1.2
		1.2010433922646726 - 0.49748846207461755i
		0.8485281374238559 - 0.8485281374238581i
		0.6888301782571612 - 1.6629831585203163i
		-6.737104846901913E-15 - 2.5i
		-1.7986121321159287 - 4.342233802803046i
		-2.2627416997969534 - 2.2627416997969507i
		-2.956414504036117 - 1.2245869835682892i
		-3.2 + 1.72469884080689E-14i
		-5.450889241816582 + 2.257832250954053i
		-5.1618795026617805 + 5.161879502661813i
		-2.717052369792077 + 6.559544680830161i
		9.790984586812942E-15 + 10.0i
		4.630469531617505 + 11.178942343386602i
		6.505382386916229 + 6.505382386916244i
		8.961631465359499 + 3.7120292939413275i
		*/
		
		ArrayList<Integer> keys = new ArrayList<Integer>(result.keySet());
		Collections.sort(keys);
		System.out.print("result: ");
		for (int i:keys) {
			System.out.println(i + " " + result.get(i) + " ");
		}
	}
}
