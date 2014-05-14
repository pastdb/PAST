/* Author - Puneet Sharma, Sciper ID - 234277
 * Towards the completion of Big data mini project
 * 
 * Description -This class is responsible for doing gridding and sampling on the compressed segments. For a perticular query it grids all the qualified segments
 * in a parallel and provides all the required values. 
 */

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
//import org.apache.spark.mllib.regression.LabeledPoint;
import java.text.*;
import java.io.*;
import java.util.*;

public class decompress {
	
	// java spark context object
	static JavaSparkContext ctx;
	
	// sets the feature type for gridding
	static String feature_type = "polynomial"; // "time" can be alternative
	
	// time format of input query string
	static String time_format = "yyyy/MM/dd:HH:mm:ss";
	
	// set it to true if the input query provide unix timestamps
	static boolean timestamps = false;
	
	// set the granularity to grid for polynomial features
	static double polynomial_granularity = 1.0;
	
	// set the granulaity to grid for time features
	static int time_granularity = 1000; //in milliseconds
	
	// sets the query type
	static String query_type = "time_point"; // "time_range", "value_point", "value_range", "composite" -- these are other options
	
	//for time range and composite query
	static String query_time_start;
	static String query_time_end;
	
	// for value range and composite query
	static String query_value_start;
	static String query_value_end;
	
	// for time and value point queries
	static String query_time;
	static String query_value;
	
	// contructor which sets the spark context
	public decompress(JavaSparkContext ctx){
		decompress.ctx = ctx;
	}
	
	// for each segments returns the set of qualifies results
	public static class grid_model extends FlatMapFunction <List<String>, String> {
		
		private static final long serialVersionUID = -7630223385777784923L;
		
		
		// get time features from the time stamp
		public List<Integer> get_features(long input) throws Exception{
			
			List<Integer> out = new ArrayList<Integer>();
			
			Date d = new Date(input);
			
			Calendar c = Calendar.getInstance();
			c.setTime(d);
			
			out.add(c.get(c.YEAR));
			out.add(c.get(c.MONTH));
			out.add(c.get(c.WEEK_OF_MONTH));
			out.add(c.get(c.DAY_OF_WEEK));
			out.add(c.get(c.HOUR_OF_DAY));
			out.add(c.get(c.MINUTE));
			out.add(c.get(c.SECOND));
			out.add(1);
			
			return out;
			
		}
		
		// gets the timestamp from the query
		public long get_timestamp(String input) throws Exception{
			
			long ts;
			
			if(timestamps == true){
				ts = Long.parseLong(input);
			}else{
				
				SimpleDateFormat fmt = new SimpleDateFormat(decompress.time_format);
				ts = fmt.parse(input).getTime();
			}
			return ts;
		}
		
		
		// ain function to be called upton each segment
		public Iterable<String> call(List<String> input)throws Exception{
		
			List<String> output = new ArrayList<String>();
			
			// fr polynomial features
			if(feature_type.equals("polynomial")){
				
				double t_left = Double.parseDouble(input.get(0));
				double t_right = Double.parseDouble(input.get(1));
				
				// time point query
				if(query_type.equals("time_point")){
					
					double time = Double.parseDouble(query_time);
					
					if(t_left <= time && time <= t_right ){
						
						double val = 0;
						for(int i = 2; i < input.size(); i++){
							val = val + Double.parseDouble(input.get(i)) * Math.pow(time, input.size() - i - 1);
						}
						
						output.add(time + "," + val);
					}
				
				// time range query
				}else if(query_type.equals("time_range")){
					
					double t_start = Double.parseDouble(query_time_start);
					double t_end = Double.parseDouble(query_time_end);
					
					for(double d = t_left; d <= t_right; d = d + polynomial_granularity){
						
						if(t_start <= d && d <= t_end ){
							
							double val = 0;
							
							for(int i = 2; i < input.size(); i++){
								val = val + Double.parseDouble(input.get(i)) * Math.pow(d, input.size() - i - 1);
							}
							
							output.add(d + "," + val);
						}
						
					}
					
				// value point query	
				}else if(query_type.equals("value_point")){
					
					double value = Double.parseDouble(query_value);
					
					for(double d = t_left; d <= t_right; d = d + polynomial_granularity){
						
						double val = 0;
						
						for(int i = 2; i < input.size(); i++){
							val = val + Double.parseDouble(input.get(i)) * Math.pow(d, input.size() - i - 1);
						}
						
						if(val == value){
							output.add(d + "," + value);
						}
						
					}
							
				// value range query
				}else if(query_type.equals("value_range")){
					
					double v_start = Double.parseDouble(query_value_start);
					double v_end = Double.parseDouble(query_value_end);
					
					for(double d = t_left; d <= t_right; d = d + polynomial_granularity){
						
						double val = 0;
						
						for(int i = 2; i < input.size(); i++){
							val = val + Double.parseDouble(input.get(i)) * Math.pow(d, input.size() - i - 1);
						}
						
						if(v_start <= val && val <= v_end){
							output.add(d + "," + val);
						}
						
					}
				
				//composite query
				}else if(query_type.equals("composite")){
					
					double t_start = Double.parseDouble(query_time_start);
					double t_end = Double.parseDouble(query_time_end);
					double v_start = Double.parseDouble(query_value_start);
					double v_end = Double.parseDouble(query_value_end);
					
					for(double d = t_left; d <= t_right; d = d + polynomial_granularity){
						
						if(t_start <= d && d <= t_end){
							double val = 0;
						
							for(int i = 2; i < input.size(); i++){
								val = val + Double.parseDouble(input.get(i)) * Math.pow(d, input.size() - i - 1);
							}
						
							if(v_start <= val && val <= v_end){
								output.add(d + "," + val);
							}
						}
						
					}
					
				}
				
			// if feature_type = "time", i.e. time features are used for compression
				
			}else if(feature_type.equals("time")){
				
				long t_left = Long.parseLong(input.get(0));;
				long t_right = Long.parseLong(input.get(1));
				
				if(query_type.equals("time_point")){
					
						long time = get_timestamp(query_time);
						List<Integer> features = get_features(time);
						
						if(t_left <= time && time <= t_right){
							
							double val = 0;
							
							for(int i = 2; i < input.size(); i++){
								val = val + Double.parseDouble(input.get(i)) * features.get(i - 2);
							}
							output.add(time + "," + val);
						}
						
				}else if(query_type.equals("time_range")){
					
					long t_start = get_timestamp(query_time_start);
					long t_end = get_timestamp(query_time_end);
					
					for(long d = t_left; d <= t_right; d = d + time_granularity){
						
						if(t_start <= d && d <= t_end ){
							
							double val = 0;
							List<Integer> features = get_features(d);
							
							for(int i = 2; i < input.size(); i++){
								val = val + Double.parseDouble(input.get(i)) * features.get(i - 2);
							}
							
							output.add(d + "," + val);
						}
						
					}
					
				
				}else if(query_type.equals("value_point")){
					
					double value = Double.parseDouble(query_value);
					
					for(long d = t_left; d <= t_right; d = d + time_granularity){
						
						double val = 0;
						List<Integer> features = get_features(d);
						
						for(int i = 2; i < input.size(); i++){
							val = val + Double.parseDouble(input.get(i)) * features.get(i - 2);
						}
						
						if(val == value){
							output.add(d + "," + value);
						}
						
					}
					
				}else if(query_type.equals("value_range")){
					
					double v_start = Double.parseDouble(query_value_start);
					double v_end = Double.parseDouble(query_value_end);
					
					for(long d = t_left; d <= t_right; d = d + time_granularity){
						
						double val = 0;
						List<Integer> features = get_features(d);
						
						for(int i = 2; i < input.size(); i++){
							val = val + Double.parseDouble(input.get(i)) * features.get(i - 2);
						}
						
						if(v_start <= val && val <= v_end){
							output.add(d + "," + val);
						}
						
					}
					
				
				}else if(query_type.equals("composite")){
					
					long t_start = get_timestamp(query_time_start);
					long t_end = get_timestamp(query_time_end);
					double v_start = Double.parseDouble(query_value_start);
					double v_end = Double.parseDouble(query_value_end);
					
					for(long d = t_left; d <= t_right; d = d + time_granularity){
						
						if(t_start <= d && d <= t_end){
							
							double val = 0;
							List<Integer> features = get_features(d);
							
							for(int i = 2; i < input.size(); i++){
								val = val + Double.parseDouble(input.get(i)) * features.get(i - 2);
							}
							
							if(v_start <= val && val <= v_end){
								output.add(d + "," + val);
							}
						}
						
					}
				
				}
			}
			
			return output;
		}
	}
			
	// call this function with a RDD object of segments
	// each segment is - T_start, T_end, coefficients in a list object
	public JavaRDD<String> get_data(JavaRDD<List<String>> input){
		
		JavaRDD<String> output = input.flatMap(new grid_model());
		
		return output;
		
	}
	
	//main function to test all
	public static void main(String args[]) throws Exception{
		
		String master_url = "local[8]";
		String job_name = "decompression";
		String spark_home = "SPARK_HOME";
		
		JavaSparkContext sc = new JavaSparkContext(master_url, job_name, System.getenv(spark_home), JavaSparkContext.jarOfClass(decompress.class));
		
		
		// example for polynomial feature -- composite query
		
		/*decompress.query_type = "composite";
		decompress.query_value_start = "45";
		decompress.query_value_end = "55";
		decompress.query_time_start = "5";
		decompress.query_time_end = "7";
		List<String> model = new ArrayList<String>();
		model.add("1");model.add("10");model.add("1"); model.add("0"); model.add("1");
		
		
		*/
		
		// example for time feature -- composite query
		
		decompress.timestamps = true;
		decompress.feature_type = "time";
		decompress.query_time_start = "1166286240000";
		decompress.query_time_end = "1166286244000";
		decompress.query_value_start = "2020";
		decompress.query_value_end = "2042";
		decompress.query_type = "composite";
		
		List<String> model = new ArrayList<String>();
		model.add("1166286240000");model.add("1166310540000");model.add("1"); model.add("0"); model.add("1");
		model.add("1"); model.add("0"); model.add("1");model.add("1"); model.add("0");
		
		List<List<String>> input = new ArrayList<List<String>>();
		input.add(model);
		JavaRDD<List<String>> input_models = sc.parallelize(input);
		
		
		decompress dc = new decompress(sc);
		
		JavaRDD<String> result =  dc.get_data(input_models); // result obtained
		
		List<String> pn = result.collect(); // to print the result
		
		for(int i = 0; i < pn.size(); i++){
			System.out.println(pn.get(i));
		}
	}
	

}
