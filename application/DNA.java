
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DNA {

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			String toFind = "GGCAATGGCCAGGATATTAGAACAGTACTCTGTGAACCCTATTTATGGTAGCACCCCTTAGACTAAGATAACACAGGGAGCAAGAGGTTGACAGGAAAGCCAGGGGAGCAGGGAAGCCTCCTGTAAAGAGAGAAGTGCTAAGTCTCCTTTCTAAGGCACATGATGGATTCAAGGGAAAGTCACATTTGACTAAAGCCCAAGGGATTGTTGCTTCTAATCCGATTCTTGGCAGAAGATATTGCAAACTAAGAGTCAGATTAATATGTGGGTGCCAAAATAAATAAACAAATAATTGAATAATCCCTGGAGGTTTAAGTGAGGAGAAACTCCTCCACAGCTTGCTACCGAGGCAGAACCGGTTGAAACTGAAATGCACCCGCTGCCAGAGGATCTGTAAAAGGGAGGTTGTTACCGAACTGGCAACTGCCAACCAAAGTCTACCAATGGACAAGCAAAAAAGAGCACTCATCTCATGCTCCCAAGGATCAACCTTCCCAGAATTTTCACTTAAGTGGCCACCAAGCCAGTTGTCAATCCAGGGCTTTGGACTGAAATCTAGGGCTTCATCCACTACCTCAGAGTGTCTTCCATTTCTTCCAGCCAGTGACAAATACAACAAACATCTGAGATGTTTTAGCTATAAATCCTTTACAATTGTTATTTATGTCTTAACTTTTGTTATACCTGGAAAAGTAGGGGAAACAATAAGAACATACTGTCTTGGCCAAGCATCCAAGGTTAAATGAGTTATGGGAATTCATTTGGGAGCCAAGACATTGCGCGTGGTTATTTATTAGTCACCCAAGCATGTATTTTGCATGTCCATCAGTTGTTCTTGGCCAAAAGAACAGAATCAATGAGCCGCTGCAGATGCAGACATAGCAGCCCCTTGCAGGAACAAGTCTGCAAGATGAGCATTGAAGAGGATGCACAAGCCCGGTAGCCCGGGAAATGGCAGGCACTTACAAGAGCCCAGGTTGTTGCCATGTTTGTTTTTGCAACTTGTCTATTTAAACAGATTTGA";
			String result = "";
			int closeness = 0;

			for (int i = 0; i < line.length() - toFind.length(); i++) {
				int a = computeDifference(
						(String) line.subSequence(i, i + toFind.length()),
						toFind);
				if (closeness > a) {
					result = (String) line.subSequence(i, i + toFind.length());
					closeness = a;
				}
			}

			StringTokenizer tokenizer = new StringTokenizer(result);
			private final static IntWritable one = new IntWritable(closeness);

			word.set(tokenizer.nextToken());
			context.write(word, one);

		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum = val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "DNA");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

	public static int value(char c) {
		if (c == 'A' || c == 'a') {
			return 2;
		} else if (c == 'G' || c == 'g') {
			return 1;
		} else if (c == 'C' || c == 'c') {
			return -1;
		} else if (c == 'T' || c == 't') {
			return -2;
		} else {
			return 0;
		}
	}

	public static int computeDifference(String a, String b) {
		int result = 0;
		for (int i = 0; i < a.length(); i++) {
			result += value(a.charAt(i)) - value(b.charAt(i));
		}

		return result;
	}

}