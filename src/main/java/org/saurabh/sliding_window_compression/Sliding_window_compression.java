/*
Code Author: Saurabh Jain (saurabh.jain@epfl.ch)


*/

import java.util.*;

import org.apache.spark.Partition;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import org.apache.spark.mllib.regression.*;
import org.apache.spark.mllib.classification.*;



public class Sliding_window_compression {
	
	// Tunable variables for compression.
 	static JavaSparkContext spark_java_context;
	
 	// This variable defines the initial size as well as the minimum size of the modelled segments. 
 	static int size_initial_segment = 10;
	
 	// This variable is the size by which the current window size is extended while merging different segments.  
 	static int size_merge_segment = 5;
	
 	// This variable defines the mean absolute error tolerance while modelling the segments. 
 	static double MAE_tolerance = 1;

 	// This variable defines the degree of polynomial to fit for the regression.
 	static int degree_polynomial = 2;
	
 	// This variable defines the step size for the gradient descent optimization. 
 	double step_size = 1.0;
	
 	// This variable defines the maximum number of iterations used by Gradient Descent. 
 	int no_of_iterations = 20;
	
	
 	// This is the main class for implementation of sliding window compression algorithm.
 	public Sliding_window_compression(JavaSparkContext spark_java_context){
 		Sliding_window_compression.spark_java_context = spark_java_context;
 	}
	
 	// This class defines the structure of a segment model. 
 	public class segment_model{
		
 		double time_left;
 		double time_right;
 		double value_min;
 		double value_max;
 		double value_mean;
 		double MAE;
 		double[] regression_coefficients;
 		double regression_intercept;
 		double length;	
 	}
	
 	// This method generates LabeledPoint object needed by Mlib(spark) for fitting a regression model. 
 	public static class feature_value_pair_generation extends Function <String, LabeledPoint> {
		
 		private static final long serialVersionUID = -7630223385777784923L;
		
 		public LabeledPoint call(String s){
				
 			String input[] = s.split(",");
				
 			double time = Double.parseDouble(input[0]);
 			double val = Double.parseDouble(input[1]);
				
 			double features[] = new double[degree_polynomial];
				
 			for(int i = degree_polynomial; i >= 1 ; i--){
 				features[degree_polynomial - i] = Math.pow(time, i);
 			}
				
 			return new LabeledPoint(val, features);
			
 		}
	
 	}
	
 	// This method is used to train a regression model on data segments using Mlib library from Spark on 
 	// LabeledPoint objects(containing feature and values) generated.
 	public segment_model train_model(JavaRDD<LabeledPoint> input, double tl, double tr) throws Exception{
		
 		segment_model model_seg = new segment_model();
		
 		List<LabeledPoint> training_data = input.collect();
		
 		// Lasso regularization Regression method with Stochastic Gradient Descent has been used with default
 		// or user supplied parameters. 
 		LassoWithSGD m = new LassoWithSGD();
 		m.optimizer().setStepSize(step_size);
 		m.optimizer().setRegParam(0.0);
 		m.optimizer().setNumIterations(no_of_iterations);
		
 		LassoModel model = m.run(input.rdd());
		
 		model_seg.regression_coefficients = model.weights();
 		model_seg.regression_intercept = model.intercept();
 		model_seg.time_left = tl;
 		model_seg.time_right = tr;
		
 		double mean = 0.0;
 		double MAE = 0.0;
 		double value_max = Double.MIN_VALUE;
 		double value_min = Double.MAX_VALUE;
		
 		for(int i = 0; i < training_data.size(); i++){
 			double pred = model.predict(training_data.get(i).features());
 			MAE += Math.abs(pred - training_data.get(i).label());
 			mean += pred;
			
 			if(pred < value_min){
 				value_min = pred;
 			}
			
 			if(pred > value_max){
 				value_max = pred;
 			}
 		}
		
 		mean = mean/training_data.size();
 		MAE = ((MAE/training_data.size()) * 100)/mean;
		
 		model_seg.length = training_data.size();
 		model_seg.value_mean = mean;
 		model_seg.MAE = MAE;
 		model_seg.value_min = value_min;
 		model_seg.value_max = value_max;
		
 		return model_seg;
		
 	}
	
 	// This function is the parent function calling all other functions in the correct order for compressing the 
 	// data after checking the arguements provided by users. 
 	public void compress(String args[]) throws Exception{
		
 		if(args.length < 2){
 			System.out.println("Please enter at least two arguments.");
 			throw new Exception();
 		}
		
 		if(args.length > 2){
 			Sliding_window_compression.degree_polynomial = Integer.parseInt(args[2]);
			
 			if(Sliding_window_compression.degree_polynomial < 1){
 				System.out.println("Value of variable degree needs to be greater than 1");
 				throw new Exception();
 			}
 		}
		
 		if(args.length > 3){
 			Sliding_window_compression.MAE_tolerance = Double.parseDouble(args[3]);
 		}
		
 		if(args.length > 4){
 			Sliding_window_compression.size_initial_segment = Integer.parseInt(args[4]);
 		}
		
 		if(args.length > 5){
 			Sliding_window_compression.size_merge_segment = Integer.parseInt(args[5]);
 		}
		
 		JavaRDD<String> input_data = spark_java_context.textFile(args[0]).cache();
		
 		List<Partition> data_partitions = input_data.splits();
		
 		Vector<segment_model> segments = new Vector<segment_model>();
		
 		for(int i = 0; i < data_partitions.size(); i++){
			
 			List<String> split = input_data.collectPartitions(new int[]{i})[0];
 			List<Double> timestamps = new ArrayList<Double>();
			
 			for(int j = 0; j < split.size(); j++){
 				timestamps.add(Double.parseDouble(split.get(j).split(",")[0]));
 			}
			
			
 			int left = 0;
 			int right = left;
 			int right_prv = right;
			
 			segment_model model_so_far = new segment_model();
 			boolean stop = false;
 			boolean initialize = false;
			
 			while(left < split.size() && stop == false){
				
 				if(initialize == true){
 					right_prv = right;
 					right = right + size_merge_segment;
 				}else{
 					initialize = true;
 					right_prv = right;
 					right = left + size_initial_segment;
 				}
				
 				if(right > split.size()){
 					right = split.size();
 					stop = true;
 				}
				
 				List<String> sub_data = split.subList(left, right);
				
 				JavaRDD<LabeledPoint> train_data = spark_java_context.parallelize(sub_data).map(new feature_value_pair_generation());
				
 				segment_model model = train_model(train_data, timestamps.get(left), timestamps.get(right - 1));
				
 				if(model.MAE >= MAE_tolerance){
					
 					if(right - left == size_initial_segment){
 						segments.add(model);
 						left = right;
 					}else{
 						segments.add(model_so_far);
 						left = right_prv;
 					}
 					initialize = false;
					
 				}else{
					
 					if(stop == true){
 						segments.add(model);
 					}else{
 						model_so_far = model;
 					}
 				}
 			}
 		}
			
 		List<String> output = new ArrayList<String>();
			
 		for(int j = 0; j < segments.size(); j++){
				
 			segment_model seg = segments.get(j);
 			String out = seg.time_left + "," + seg.time_right + "$" + seg.value_min + "," + seg.value_max + "$" + seg.regression_coefficients[0];
				
 			for(int k = 1; k < seg.regression_coefficients.length; k++){
 				out = out + "," + seg.regression_coefficients[k]; 
 			}
				
 			out = out + "," + seg.regression_intercept;
				
 			out = out + "$" + seg.value_mean + "," + seg.length;
				
 			output.add(out);
 		}
			
 		JavaRDD<String> final_output = spark_java_context.parallelize(output);
			
 		final_output.saveAsTextFile(args[1]);
 	}
		
	
 	public static void main(String args[]) throws Exception{
		
 		String input_path = "/home/saurabh/data.txt";
 		String output_path = "/home/saurabh/output";
 		String master_url = "local[3]";
 		String job_name = "Sliding_Window_Compression";
 		String spark_home = "SPARK_HOME";
		
 		JavaSparkContext spark_java_context = new JavaSparkContext(master_url, job_name, System.getenv(spark_home), JavaSparkContext.jarOfClass(Sliding_window_compression.class));
		
 		Sliding_window_compression compression = new Sliding_window_compression(spark_java_context);
		
 		compression.compress(new String[]{input_path, output_path});
		
 	}
	
 }
