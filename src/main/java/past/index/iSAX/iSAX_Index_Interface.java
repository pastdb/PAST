package past.index.iSAX;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.*;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import com.typesafe.config.Config; 
import com.typesafe.config.ConfigFactory;



public interface iSAX_Index_Interface{


/** Configure the index
* @param timestamp the RDD representing the timestamp of the timeseries
* @param values the RDD representing the values of the timeseries
* @param ts_name the name of the timeseries (it's unique identifier)
* @param path the Path to the file representing the timeseries
* @param cardinality the cardinality that is used to form the iSAX representation of the data; must be a power of two 
* @param th threshold determining the maximum number of timeseries that a terminal index node can refere to
* @param j_sc_context  the JavaSparkContext
* @param start the start of the 
* @param stop  the JavaSparkContext
**/
public void configure_iSAX_index_RDD(JavaRDD<Integer> timestamp,JavaRDD<Double> values, String ts_name, Path path,int cardinality, int th, int word_length,JavaSparkContext j_sc_context,int start,int stop);

/** Insert a new timeseries into a index that has previously been configured with configure_iSAX_index_RDD
* @param timestamp the RDD representing the timestamp of the timeseries
* @param values the RDD representing the values of the timeseries
* @param ts_name the name of the timeseries (it's unique identifier)
* @param path the Path to the file representing the timeseries
**/
public void insert_raw_RDD(JavaRDD<Integer> timestamp,JavaRDD<Double> values, String ts_name, Path path);

}
