package past.index.iSAX;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.*;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkContext;
import org.apache.hadoop.util.Progressable;


import java.util.*;
import java.io.*;
import past.Timeseries;
import past.Transformations;
import past.storage.*;
import past.storage.DBType;
import java.awt.Point;
import past.index.iSAX.TreeNode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import com.typesafe.config.Config; 
import com.typesafe.config.ConfigFactory;

import java.net.URI;
import java.net.URISyntaxException;

public class iSAX_Index  implements iSAX_Index_Interface{

//Word length for SAX representation _ dimension
private int word_length;

//Base Cardinality to which we reduce the timeseries
private int cardinality;

//User defined threshold that limits the number of timeseries in one file; Causes a node split
private int th;

private int dimension;
//Start of timeseries window. Default =0
private int start;
//End of timeseries window. Default = 100
private int stop;

private TreeNode tree;

private Timeseries ts;
private JavaSparkContext sc;
 FileSystem fs;
 String name;
String configPath;

public int getCardinality(){
	return this.cardinality;
}

public int getWordLength(){
	return this.word_length;
}
public int getStop(){

	return this.stop;
}
public int getStart(){
	return this.start;
}
/**
* Configure the index - called from the application that is using the index

* @param cardinality the cardinality of the iSAX word. Default = 4
* @param th threshold used to limit the number of timeseries in a file. Default = 100
* @param word_length the length of the word used by the iSAX representation. Default = 4
**/
public void configure_iSAX_index(int cardinality,  int th, int word_length){
	
	this.cardinality=cardinality;
	this.th=th;
	this.word_length=word_length;
	
}

/**
*	Configures the index from JavaRDDs
*
*
**/

public void configure_iSAX_index_RDD(JavaRDD<Integer> timestamp,JavaRDD<Double> values, String ts_name, Path path,int cardinality, int th, int word_length,JavaSparkContext j_sc_context,int start, int stop){
	List<Integer> timestamp_list = timestamp.collect();
	List<Double> value_list = values.collect();
	Hashtable<Integer,java.lang.Object> ts_data = new Hashtable<Integer,java.lang.Object>();
	for(int i=0;i<timestamp_list.size();i++){
		if(i%2==0)
		ts_data.put(timestamp_list.get(i),value_list.get(i));

	}
	Hashtable<java.lang.String,Hashtable<Integer,java.lang.Object>> ts_hash= new Hashtable<java.lang.String,Hashtable<Integer,java.lang.Object>>();
	ts_hash.put(ts_name,ts_data);
	Timeseries t=new Timeseries(ts_name,ts_hash,DBType.DBInt32$.MODULE$);
	this.sc=j_sc_context;
	this.set_dataset(t,this.sc);
	this.cardinality=cardinality;
	this.th=th;
	TreeNode.th_max=th;
	this.word_length= word_length;
	this.start=start;
	this.stop=stop;
	
}

public iSAX_Index() throws URISyntaxException,IOException {
	this.configure_iSAX_index(4,2,4);	
	this.start =0;
	this.stop=50;
	this.tree=new TreeNode(this.cardinality,this.word_length, TreeNode.NodeType.ROOT,this.cardinality);
	TreeNode.th_max=th;	
	fs = FileSystem.get(new URI("file:///tmp"), new Configuration());
}

public iSAX_Index(int cardinality,  int th, int word_length, int start, int stop)  throws URISyntaxException,IOException {
	this.configure_iSAX_index(cardinality,th,word_length);
	this.th=th;
	this.start=start;
	this.stop=stop;
	this.tree=new TreeNode(this.cardinality,this.word_length, TreeNode.NodeType.ROOT,this.cardinality);	
	TreeNode.th_max=th;
	fs = FileSystem.get(new URI("file:///tmp"), new Configuration());
}

public void set_dataset(Timeseries ts, JavaSparkContext sc){
	this.ts = ts;
	this.sc=sc;
	this.configPath=ts.getPath().getParent().toString();
	this.name=ts.getPath().getName()+".sax";
	
}

public void setStartStop(int start,int stop){
	this.start = start;
	this.stop = stop;
}

public void setTh(int threshold){
	this.th=threshold;
	
}
//TODO check if index for TS already exists
public static String getSAXString(Hashtable<Integer,Point> SAX,int card){
	
	Enumeration<Point> saxWordStruct = SAX.elements();
	String saxWord="";
		while(saxWordStruct.hasMoreElements()){
			String tmp=Integer.toBinaryString(saxWordStruct.nextElement().x);
			if (tmp.length()==1){
				for(int i=iSAX_dist_utils.log2(card)-1;i>0;i--){
					saxWord+="0";
				}
			}
			saxWord+=tmp;
		}
		
	return saxWord;
}
public boolean build_index(){
	System.out.println("...building index....:");
	return insert_raw(ts);
}



private void print_index(int i, TreeNode node){
System.out.println("Printing .... LEVEL:"+i);
System.out.println("Node level: "+node.myType);
System.out.println("Printing children of : "+i);
for (Map.Entry<String, Path> entry2 : node.indexed_timeseries.entrySet()) {
			System.out.println("TS::"+entry2.getKey().toString()+"::PATH::"+entry2.getValue().toString());
		}
 for (Map.Entry<String, TreeNode> entry : node.children.entrySet()) {
			System.out.println("SAX word:" + entry.getKey().toString()+ " ......... NODE::TYPE::"+entry.getValue().myType);
		//for (Map.Entry<String, Path> entry2 : node.indexed_timeseries.entrySet()) {
		//	System.out.println("TS::"+entry2.getKey().toString()+"::PATH::"+entry2.getValue().toString());
		//}
		print_index(i+1,entry.getValue());	
	}
}
public boolean insert_raw(Timeseries ts){
Hashtable<Integer,Point> SAX = Transformations.symbolicAggregateApproximation(ts, ts.getPath().getName(), start, stop, word_length, cardinality);
boolean inserted=insert(SAX,ts);
	
	if(!inserted){
		System.out.println("Failed to insert node!");
		System.exit(1);
		}
		
	else{
		print_index(0,tree);
	}
	return inserted;

}


public void insert_raw_RDD(JavaRDD<Integer> timestamp,JavaRDD<Double> values, String ts_name, Path path){
	List<Integer> timestamp_list = timestamp.collect();
	List<Double> value_list = values.collect();
	Hashtable<Integer,java.lang.Object> ts_data = new Hashtable<Integer,java.lang.Object>();
	for(int i=0;i<timestamp_list.size();i++){
		if(i%2==0)
		ts_data.put(timestamp_list.get(i),value_list.get(i));

	}
	Hashtable<java.lang.String,Hashtable<Integer,java.lang.Object>> ts_hash= new Hashtable<java.lang.String,Hashtable<Integer,java.lang.Object>>();
	ts_hash.put(ts_name,ts_data);
	Timeseries t=new Timeseries(ts_name,ts_hash,DBType.DBInt32$.MODULE$);
	
	this.set_dataset(t,this.sc);
	this.insert_raw(t);
}
private boolean insert(Hashtable<Integer,Point> SAX , Timeseries ts){

String saxWord=iSAX_Index.getSAXString(SAX,cardinality);
Path file = new Path(configPath,name);
try{

System.out.println(saxWord);

//saves the timeseries that is being indexed to disk in a separate file; to be used for clustering 

if(fs.exists(file)) {
 fs.delete(file,true);
}
OutputStream os= fs.create(file, new Progressable() {
	public void progress(){
		System.out.println("..writting..");
	}
});
BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));


	for (String key:ts.getTimeseries().keySet()){
		Hashtable<Integer,Object> ts_hash = ts.getTimeseries().get(key);
		for(Integer key2:ts_hash.keySet()){
			br.write(key2.toString());
			br.write(" ");
			br.write(ts_hash.get(key2).toString());
			br.newLine();
		}
	}
	br.close();
}catch (IOException e){
	System.out.println(e);
}
return tree.insert(saxWord,ts.getName(),file,cardinality);

//rn true;
}


public TreeNode getRoot(){	
	return this.tree;
}



public static Timeseries convertRDD2Timseries(JavaRDD<Integer> timestamp,JavaRDD<Double>values,String ts_name){

	List<Integer> timestamp_list = timestamp.collect();
	List<Double> value_list = values.collect();
	Hashtable<Integer,java.lang.Object> ts_data = new Hashtable<Integer,java.lang.Object>();
	for(int i=0;i<timestamp_list.size();i++){
		if(i%2==0)
		ts_data.put(timestamp_list.get(i),value_list.get(i));

	}
	Hashtable<java.lang.String,Hashtable<Integer,java.lang.Object>> ts_hash= new Hashtable<java.lang.String,Hashtable<Integer,java.lang.Object>>();
	ts_hash.put(ts_name,ts_data);
	Timeseries t=new Timeseries(ts_name,ts_hash,DBType.DBInt32$.MODULE$);
	return t;
}
public  double ExactSearchRDD(iSAX_Index index, JavaRDD<Integer> timestamp,JavaRDD<Double> values, String ts_name, int start, int stop){
	Timeseries ts=iSAX_Index.convertRDD2Timseries(timestamp,values,ts_name);
	return iSAXQuery.ExactSearch(index,ts,ts_name,start,stop);

}
public  double ApproximateSearchRDD(iSAX_Index index, JavaRDD<Integer> timestamp,JavaRDD<Double> values,String ts_name, int start, int stop){
	Timeseries ts=iSAX_Index.convertRDD2Timseries(timestamp,values,ts_name);
	return iSAXQuery.ApproximateSearch(index,ts,start,stop);

}

}
