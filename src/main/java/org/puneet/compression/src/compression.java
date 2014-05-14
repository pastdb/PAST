import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import weka.classifiers.functions.LinearRegression;
import weka.core.Instances;
import java.text.*;
import java.io.*;
import java.util.*;


//arg 1 - input path - string
//arg 2 - output path - string


public class compression {
	
	static JavaSparkContext ctx;
	
	/*set of default parameters which can be tuned*/
	
	static double max_MAE = 20;
	static int min_seg_length = 2;
	
	static String compression_type = "regression"; 			// alernative "APCA" 
	static String feature_type = "polynomial"; 				// alternative is "time"
	static int degree = 2; 									// set this if feature_type is polynomial
	
	static String time_format = "yyyy/MM/dd:HH:mm:ss";
	static boolean timestamps = false;
	
	//no of partitions for data -- change it if need to change the default partitioning by 
	static int partitions = 0;
	
	//APCA specific
	static int APCA_seg_count = 10000;
	
	static int column_no;
	static String delimiter = ",";
	
	/* ---------------------------------------------------------------------------------------------------------------*/
	
	public compression(JavaSparkContext ctx){
		
		compression.ctx = ctx;
	}
	
	
	// compression for each partition, will return the compressed segments
	
	public static class get_compress extends FlatMapFunction <Iterator<String>, String> {
		
		private static final long serialVersionUID = -7630223385777784923L;
		
		public class segment {
			
			int left; int right;
			String t_left; String t_right;
			double v_min; double v_max;
			double merge_v_min; double merge_v_max;
			double coefficients[]; double merge_coefficients[];
			double v_mean; double merge_v_mean;
			double mae; double merge_mae;
		}
		
		
		public segment fit_model(Instances input) throws Exception{
			
			LinearRegression lin_reg = new LinearRegression();
			lin_reg.buildClassifier(input);
			
			segment model_seg = new segment();
			model_seg.coefficients = lin_reg.coefficients();
			
			double v_min = Double.MAX_VALUE;
			double v_max = Double.MIN_VALUE;
			double mae = 0.0;
			double mean = 0.0;
			double mean_real = 0.0;
			
			for(int i = 0; i < input.numInstances(); i++){
				
				double pred = lin_reg.classifyInstance(input.instance(i));
				double actl = input.instance(i).classValue();
				mae += Math.abs(pred - actl);
				mean += pred; 
				mean_real += actl;
				
				if(pred > v_max){
					v_max = pred; 
				}
				
				if(pred < v_min){
					v_min = pred;
				}
			}
			mean_real = mean_real/input.numInstances();
			model_seg.mae = ((mae/input.numInstances()) * 100)/mean_real;
			
			model_seg.v_mean = mean/input.numInstances();
			model_seg.v_min = v_min;
			model_seg.v_max = v_max;
			
			return model_seg;
			
		}
		
		
		// merging the segments and other book keeping
		
		public boolean merge_segments(Instances data, List<segment> seg_list, boolean first) throws Exception{
			
			if(seg_list.size() < 2){
				return true;
			}
			
			if(first){
				
				for(int i = 0; i < seg_list.size() - 1; i++){
					
					segment seg = seg_list.get(i);
					Instances ins = new Instances(data, seg.left, seg_list.get(i + 1).right - seg.left);
					segment model_seg = fit_model(ins);
					
					seg.merge_coefficients = model_seg.coefficients;
					seg.merge_mae = model_seg.mae;
					seg.merge_v_min = model_seg.v_min;
					seg.merge_v_max = model_seg.v_max;
					seg.merge_v_mean = model_seg.v_mean;
				}	
			
			}else{
				
				double min_cost = seg_list.get(0).merge_mae;
				int min_ind = 0;
				
				for(int i = 1; i < seg_list.size() - 1; i++){
					if(seg_list.get(i).merge_mae < min_cost){
						min_ind = i;
						min_cost = seg_list.get(i).merge_mae;
					}
				}
				
				if(min_cost > max_MAE){
					
					return true;
				
				}else{
					
					segment seg = seg_list.get(min_ind);
					seg.right = seg_list.get(min_ind + 1).right;
					seg.t_right = seg_list.get(min_ind + 1).t_right;
					seg.coefficients = seg.merge_coefficients;
					seg.mae = seg.merge_mae;
					seg.v_min = seg.merge_v_min;
					seg.v_max = seg.merge_v_max;
					seg.v_mean = seg.merge_v_mean;
					
					seg_list.remove(min_ind + 1);
					
					if(min_ind + 1 < seg_list.size()){
						
						Instances ins = new Instances(data, seg.left, seg_list.get(min_ind + 1).right - seg.left);
						segment model_seg = fit_model(ins);
						seg.merge_coefficients = model_seg.coefficients;
						seg.merge_mae = model_seg.mae;
						seg.merge_v_min = model_seg.v_min;
						seg.merge_v_max = model_seg.v_max;
						seg.merge_v_mean = model_seg.v_mean;
					}
					
					if(min_ind - 1 >= 0){
						
						segment left_seg = seg_list.get(min_ind - 1);
						Instances ins = new Instances(data, left_seg.left, seg.right - left_seg.left);
						segment model_seg = fit_model(ins);
						
						left_seg.merge_coefficients = model_seg.coefficients;
						left_seg.merge_mae = model_seg.mae;
						left_seg.merge_v_min = model_seg.v_min;
						left_seg.merge_v_max = model_seg.v_max;
						left_seg.merge_v_mean = model_seg.v_mean;
					}
					
				}
				
			}
			
			return false;
		}
		
		
		public Iterable<String> call(Iterator<String> p)throws Exception{
					
			Vector<String> time_stamps = new Vector<String>();
			String p_name = "partition_" + Math.random() * 10000 + ".arff";
			
			FileWriter fw = new FileWriter(p_name);
			fw.write("@RELATION partition \n\n");
			
			if(feature_type.equals("time")){
				
				fw.write("@ATTRIBUTE year NUMERIC\n");
				fw.write("@ATTRIBUTE month NUMERIC\n");
				fw.write("@ATTRIBUTE week_of_month NUMERIC\n");
				fw.write("@ATTRIBUTE day_of_week NUMERIC\n");
				fw.write("@ATTRIBUTE hour_of_day NUMERIC\n");
				fw.write("@ATTRIBUTE minute NUMERIC\n");
				fw.write("@ATTRIBUTE second NUMERIC\n");
				fw.write("@ATTRIBUTE value NUMERIC\n");
				
				fw.write("\n\n@DATA\n");
				
				while(p.hasNext()){
					
					String pair[] = p.next().split(delimiter);
					Date d;
					
					if(timestamps == true){
						
						d = new Date(Long.parseLong(pair[0]));
						time_stamps.add(pair[0]);
					
					}else{
						
						SimpleDateFormat fmt = new SimpleDateFormat(compression.time_format);
						d = fmt.parse(pair[0]);
						Long ts = d.getTime();
						time_stamps.add(String.valueOf(ts));
					}
					
					Calendar c = Calendar.getInstance();
					c.setTime(d);
					
					fw.write(c.get(c.YEAR) + "," + c.get(c.MONTH) + "," + c.get(c.WEEK_OF_MONTH) + "," + c.get(c.DAY_OF_WEEK) + ",");
					fw.write(c.get(c.HOUR_OF_DAY) + "," + c.get(c.MINUTE) + "," + c.get(c.SECOND) + "," + pair[1] + "\n");					
				
				}
				
			}else if (feature_type.equals("polynomial")){
				 
				for(int i = degree; i >= 0; i--){
					fw.write("@ATTRIBUTE feature_" + i + " NUMERIC\n");
				}
				
				fw.write("\n\n@DATA\n");
				
				while(p.hasNext()){
					
					String pair[] = p.next().split(delimiter);
					double time = Double.parseDouble(pair[0]);
					double value = Double.parseDouble(pair[1]);
					
					time_stamps.add(pair[0]);
					
					for(int i = degree; i >= 1; i--){
						
						fw.write(Math.pow(time, i) + ",");
					}
					
					fw.write(value + "\n");
				}
			}
			
			fw.flush();
			fw.close();
			
			FileReader fr = new FileReader(p_name);
			BufferedReader br = new BufferedReader(fr);
			
			Instances data = new Instances(br);
			br.close(); fr.close();
			data.setClassIndex(data.numAttributes() - 1);
			
			
			
			List<segment> seg_list = new ArrayList<segment>();
			
			int count = 0;
			
			while(count < data.numInstances()){
				
				int left = count;
				int right = count + min_seg_length;
				
				if(right > data.numInstances()){
					right = data.numInstances();
				}
				
				Instances seg_data = new Instances(data, left, right - left);
				segment seg = fit_model(seg_data);
				seg.left = left;
				seg.right = right;
				
				seg.t_left = time_stamps.get(left);
				seg.t_right = time_stamps.get(right - 1);
				
				seg_list.add(seg);
				
				count += min_seg_length;
				
			}
			
			boolean stop = merge_segments(data, seg_list, true);
			
			while(!stop){
				
				stop = merge_segments(data, seg_list, false);
			}
		
			List<String> return_output = new ArrayList<String>();
			
			DecimalFormat fmt = new DecimalFormat("#0.###");
			
			for(int i = 0; i < seg_list.size(); i++){
				
				segment seg = seg_list.get(i);
				
				String out = seg.t_left + "," + seg.t_right + "$" + fmt.format(seg.v_min) + "," + fmt.format(seg.v_max) + "$" + fmt.format(seg.coefficients[0]);
				
				for(int j = 1; j < seg.coefficients.length - 2; j++){
					out = out + "," + fmt.format(seg.coefficients[j]); 
				}
				
				out = out + "," + fmt.format(seg.coefficients[seg.coefficients.length - 1]);
				
				out = out + "$" + fmt.format(seg.v_mean) + "," + (seg.right - seg.left);
					
				return_output.add(out);
				
			}
			
			File f = new File(p_name);
			f.delete();
			
			return return_output;
			
		}
	
	}
	
	
	public static class get_APCA extends FlatMapFunction <Iterator<String>, String> {
		
		private static final long serialVersionUID = -7630223385777784923L;
		
		public class segment_APCA {
			
			int left; int right;
			String t_left; String t_right;
			double v_mean; double merge_v_mean;
			double mae; double merge_mae;
		}
		
		public segment_APCA fit_model(List<Double> data) throws Exception{
			
			segment_APCA model_seg = new segment_APCA();
			
			double mean = 0;
			double mae = 0;
			
			for(int i = 0; i < data.size(); i++){
				mean += data.get(i);
			}
			
			mean = mean/data.size();
			
			for(int i = 0; i < data.size(); i++){
				mae = Math.abs(mean - data.get(i));
			}
			
			mae = (mae/data.size() * 100 )/mean;
			
			model_seg.v_mean = mean;
			model_seg.mae = mae;
			
			return model_seg;
			
		}
		
		
		public boolean merge_segments_APCA(List<Double> data, List<segment_APCA> seg_list, boolean first) throws Exception{
			
			if(seg_list.size() < 2){
				return true;
			}
			
			if(first){
				
				for(int i = 0; i < seg_list.size() - 1; i++){
					
					segment_APCA seg = seg_list.get(i);
					List<Double> sub_data = data.subList(seg.left, seg_list.get(i + 1).right);
					segment_APCA model_seg = fit_model(sub_data);
					
					seg.merge_mae = model_seg.mae;
					seg.merge_v_mean = model_seg.v_mean;
				}
			
			}else{
				
				double min_cost = seg_list.get(0).merge_mae;
				int min_ind = 0;
				
				for(int i = 1; i < seg_list.size() - 1; i++){
					
					if(seg_list.get(i).merge_mae < min_cost){
						min_ind = i;
						min_cost = seg_list.get(i).merge_mae;
					}
				}
				
				segment_APCA seg = seg_list.get(min_ind);
				seg.right = seg_list.get(min_ind + 1).right;
				seg.t_right = seg_list.get(min_ind + 1).t_right;
				seg.mae = seg.merge_mae;
				seg.v_mean = seg.merge_v_mean;
				
				seg_list.remove(min_ind + 1);
				
				if(min_ind + 1 < seg_list.size()){
					
					List<Double> sub_data = data.subList(seg.left, seg_list.get(min_ind + 1).right);
					segment_APCA model_seg = fit_model(sub_data);
					seg.merge_mae = model_seg.mae;
					seg.merge_v_mean = model_seg.v_mean;
				}
				
				if(min_ind - 1 >= 0){
					
					segment_APCA left_seg = seg_list.get(min_ind - 1);
					List<Double> sub_data = data.subList(left_seg.left, seg.right);
					segment_APCA model_seg = fit_model(sub_data);
					left_seg.merge_mae = model_seg.mae;
					left_seg.merge_v_mean = model_seg.v_mean;
				}
			}
			
			return false;
			
		}
		
		public Iterable<String> call(Iterator<String> p)throws Exception{
			
			List<String> time_stamps = new ArrayList<String>();
			List <Double> data = new ArrayList<Double>();
			
			while(p.hasNext()){
				String pair[] = p.next().split(delimiter);
				if(feature_type.equals("polynomial")){
					time_stamps.add(pair[0]);
				}else{
					if(timestamps == true){
						time_stamps.add(pair[0]);
					}else{
						SimpleDateFormat fmt = new SimpleDateFormat(compression.time_format);
						Date d = fmt.parse(pair[0]);
						Long ts = d.getTime();
						time_stamps.add(String.valueOf(ts));
					}
				}
				
				data.add(Double.parseDouble(pair[1]));
			}
			
			double seg_count = APCA_seg_count;
			seg_count = Math.ceil(seg_count/partitions);
			
			int count = 0;
			
			List<segment_APCA> seg_list = new ArrayList<segment_APCA>();
			
			while(count < data.size()){
				
				int left = count;
				int right = count + min_seg_length;
				
				if(right > data.size()){
					right = data.size();
				}
				
				List<Double> seg_data = data.subList(left, right);
				segment_APCA seg = fit_model(seg_data);
				
				seg.left = left;
				seg.right = right;
				
				seg.t_left = time_stamps.get(left);
				seg.t_right = time_stamps.get(right - 1);
				
				seg_list.add(seg);
				
				count += min_seg_length;		
			
			}
			
			boolean stop = merge_segments_APCA(data, seg_list, true);
			
			while(seg_list.size() > seg_count && stop != true){
				stop = merge_segments_APCA(data, seg_list, false);
			}
			
			List<String> return_output = new ArrayList<String>();
			
			for(int i = 0; i < seg_list.size(); i++){
				
				segment_APCA seg = seg_list.get(i);
				
				String out = seg.t_left + "," + seg.t_right + "$" + seg.v_mean + "," + seg.v_mean + "$" + "0,0,0";
				
				out = out + "$" + seg.v_mean + "," + (seg.right - seg.left);
					
				return_output.add(out);
				
			}
			
			return return_output;
		}
	}
	
	public static class take_column extends Function <String, String> {
		
		private static final long serialVersionUID = -7630223385777784923L;
		
		public String call(String s){
				
			String cols[] = s.split(delimiter);
			
			return cols[0] + delimiter + cols[column_no];
		}
	
	}
	
	
	
	// function to call for the compression - minimum arguments 2
	
	List<JavaRDD<String>> do_compress(String args[]) throws Exception{
		
		if(args.length < 1){
			System.out.println("wrong no of arguments");
			throw new Exception();
		}
		
		List<JavaRDD<String>> result = new ArrayList<JavaRDD<String>>();
		
		JavaRDD<String> input;
		
		if(partitions != 0){
			input = ctx.textFile(args[0], partitions);
		
		}else{
			input = ctx.textFile(args[0]);
			compression.partitions = input.splits().size();
		}
		
		String first_row = input.first();
		int column_count = first_row.split(delimiter).length;
		
		for(int i = 1; i < column_count; i++){
			
			compression.column_no = i;
			
			if(compression_type.equals("regression")){
				
				JavaRDD<String> data = input.map(new take_column());
				JavaRDD<String> output = data.mapPartitions(new get_compress());
				if(args.length > 1){
					output.saveAsTextFile(args[1] + "/column_" + i);
				}
				result.add(output);
		
			} else if(compression_type.equals("APCA")){
				
				JavaRDD<String> data = input.map(new take_column());
				JavaRDD<String> output = data.mapPartitions(new get_APCA());
				
				if(args.length > 1){
					output.saveAsTextFile(args[1] + "/column_" + i);
				}
				result.add(output);
			}
		}
		
		return result;
		
	}
	
	public static void main(String args[]) throws Exception{
		
		String input_path = "/home/puneet/data.txt";
		String output_path = "/home/puneet/compress_output";
		String master_url = "local[8]";
		String job_name = "compression";
		String spark_home = "SPARK_HOME";
		
		JavaSparkContext sc = new JavaSparkContext(master_url, job_name, System.getenv(spark_home), JavaSparkContext.jarOfClass(compression.class));
		
		//set static compression parameters below
		
		//compression.compression_type = "APCA";
		compression.partitions = 8;
		compression.feature_type = "time";
		compression.time_format = "dd/MM/yyyy:HH:mm:ss";
		
		compression c = new compression(sc);
		c.do_compress(new String[]{input_path, output_path});
		
	}

}
