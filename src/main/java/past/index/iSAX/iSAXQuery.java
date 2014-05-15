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

import java.awt.Point;
import past.index.iSAX.TreeNode;

import past.storage.*;
import past.storage.DBType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import com.typesafe.config.Config; 
import com.typesafe.config.ConfigFactory;

import java.net.URI;
import java.net.URISyntaxException;

public class iSAXQuery{

private static class  Closest_node{
	
	private TreeNode node;
	private String sax;
	private double dist;
	
	public TreeNode getNode(){
		return node;
	}
	public String getSAX(){
		return sax;
	}
	public double getDist(){
		return dist;
	}
	public void setDist(double dist){
		this.dist=dist;
	}
	public Closest_node(String sax, TreeNode node, double dist){
		this.node = node;
		this.sax=sax;
		this.dist=dist;
	}
	
}

private static TreeNode traverseTree(TreeNode root, String SAX){
	System.out.println("Traversing ROOT");
	TreeNode node =null;
	if(root.children.containsKey(SAX)){
		System.out.println("Found in root");
		return root.children.get(SAX);
		
	}
	else
	{	
		for (Map.Entry<String, TreeNode> entry : root.children.entrySet()) {
			//System.out.println("TS::"+entry.getKey().toString()+"::PATH::"+entry.getValue().toString());
				node=iSAXQuery.traverseTree(entry.getValue(),entry.getKey());
				if(null!=node){
					break;
				}
		}
	}
	
	return node;
	
}

private static int getSAXNo(char x,char y){

	return Character.getNumericValue(x)*2+Character.getNumericValue(y);
}
private static double getMINDIST(String saxWord, String nodeKey, int cardinality,int length){
	double sum=0;
	double [][] lookupMatrix=iSAX_dist_utils.getLookupMatrix(cardinality);
	
	for(int j=0;j<cardinality;j++)
		for(int i=0;i<saxWord.length()-1;i+=2){
			int x=iSAXQuery.getSAXNo(saxWord.charAt(i),saxWord.charAt(i+1));
			
			int y=iSAXQuery.getSAXNo(nodeKey.charAt(i),nodeKey.charAt(i+1));

			sum+=lookupMatrix[x][y];	
		}

	return Math.sqrt((double)length/(double)cardinality)*(double)Math.sqrt((double)sum);
	
}
private static Closest_node find_most_similar_node(TreeNode root,int cardinality, String saxWord,int length){
	TreeNode minNode=root;
	double min_dist = -1;
	double tmp_min_dist=0;
	String minSAXKey=saxWord;
	
	for(Map.Entry<String, TreeNode> entry: root.children.entrySet()){
		if(entry.getValue().getCardinality() == cardinality){
			tmp_min_dist=iSAXQuery.getMINDIST(saxWord,entry.getKey(),cardinality,length);
			if(min_dist<0 || min_dist>tmp_min_dist){
				min_dist=tmp_min_dist;
				minNode=entry.getValue();		
				minSAXKey=entry.getKey();
			}
		}
		else
			continue;
	}
	
	
	Closest_node r_node=new Closest_node(minSAXKey, minNode,min_dist);	
	return r_node;
}
private static double getMINDIST_PAA(String saxWord, String nodeKey,int cardinality, int length,Hashtable<Integer, Object> PAA){

	double [] breakpoints = iSAX_dist_utils.getBreakpoints(cardinality);
	double sum=0;
	int i=0;
	int count =0;
	double bU=0;
	double bL=0;
	for(Map.Entry<Integer,Object>entry:PAA.entrySet()){
	if(count>=breakpoints.length) break;
		if(breakpoints.length > count+1) bU=breakpoints[count+1];
		else{
			bU=breakpoints[count];
		}
		bL=breakpoints[count];

			if(bL>(Double)entry.getValue()){
				sum+=(bL-(Double)entry.getValue())*(bL-(Double)entry.getValue());
			}
			else{
				if(bU<(Double)entry.getValue()){
					sum+=(bU-(Double)entry.getValue())*(bU-(Double)entry.getValue());
				}
				
			}
		count++;
	//}
	}
	
	return Math.sqrt((double)length/cardinality)*Math.sqrt(sum);
	

}
private static Closest_node find_most_similar_nodePAA(TreeNode root,int cardinality, String saxWord,int length,Hashtable<Integer,Object> PAA){
	TreeNode minNode=root;
	double min_dist = -1;
	double tmp_min_dist=0;
	String minSAXKey=saxWord;
	
	for(Map.Entry<String, TreeNode> entry: root.children.entrySet()){
		if(entry.getValue().getCardinality() == cardinality){
			tmp_min_dist=iSAXQuery.getMINDIST_PAA(saxWord,entry.getKey(),cardinality,length,PAA);
			if(min_dist<0 || min_dist>tmp_min_dist){
				min_dist=tmp_min_dist;
				minNode=entry.getValue();		
				minSAXKey=entry.getKey();
			}
		}
		else
			continue;
	}
	
	
	
	return new Closest_node(minSAXKey, minNode,min_dist);	
}


private static Closest_node  ApproximateSearch(iSAX_Index index, Timeseries ts,int start, int stop,boolean do_PAA,Hashtable<Integer, Object> PAA){


	int cardinality = index.getCardinality();
		int word_length = index.getWordLength();
		System.out.println("card::"+cardinality+"::word::"+word_length+"::name::"+ts.getPath().getName()+"::start::"+start+"::stop::"+stop);
		Hashtable<Integer,Point> SAX = Transformations.symbolicAggregateApproximation(ts, ts.getPath().getName(), start, stop, word_length, cardinality);		
		String saxWord=iSAX_Index.getSAXString(SAX,index.getCardinality());	
		System.out.println("Searching for "+saxWord);
		TreeNode node=iSAXQuery.traverseTree(index.getRoot(),saxWord);// get the note with the matching Hashcode
		
		if(null==node){
		System.out.println("Exact match not found");
		double dist=0;
		Closest_node result_node=iSAXQuery.find_most_similar_nodePAA(index.getRoot(),cardinality,saxWord,index.getStop()-index.getStart(),PAA);
		String result = result_node.getSAX();
			//for(Map.Entry<String,TreeNode> entry : result.entrySet()){
			if(do_PAA)
				dist= iSAXQuery.getMINDIST_PAA(saxWord,result,cardinality,index.getStop()-index.getStart(),PAA);
			else {
				dist= iSAXQuery.getMINDIST(saxWord,result,cardinality,index.getStop()-index.getStart());
			}
				//break;
				result_node.setDist(dist);
			//}
			return result_node ;		
			
		}
		Closest_node r_node=new Closest_node(saxWord,node,iSAXQuery.getMINDIST(saxWord,saxWord,cardinality,index.getStop()-index.getStart()));
		return r_node;
	
}
public static double ApproximateSearch(iSAX_Index index, Timeseries ts,int start, int stop){
		
		int cardinality = index.getCardinality();
		int word_length = index.getWordLength();
		System.out.println("card::"+cardinality+"::word::"+word_length+"::name::"+ts.getPath().getName()+"::start::"+start+"::stop::"+stop);
		Hashtable<Integer,Point> SAX = Transformations.symbolicAggregateApproximation(ts, ts.getPath().getName(), start, stop, word_length, cardinality);		
		String saxWord=iSAX_Index.getSAXString(SAX,index.getCardinality());	
		System.out.println("Searching for "+saxWord);
		TreeNode node=iSAXQuery.traverseTree(index.getRoot(),saxWord);// get the note with the matching Hashcode
		
		if(null==node){
		System.out.println("Exact match not found");
		double dist=0;
		Closest_node result_node=iSAXQuery.find_most_similar_node(index.getRoot(),cardinality,saxWord,index.getStop()-index.getStart());
		String result = result_node.getSAX();

				dist= iSAXQuery.getMINDIST(saxWord,result,cardinality,index.getStop()-index.getStart());
		
			return dist;		
			
		}
		else{
			return iSAXQuery.getMINDIST(saxWord,saxWord,cardinality,index.getStop()-index.getStart());
		}
	}


public static double ExactSearch(iSAX_Index index, Timeseries ts,String name,int start, int stop){
		int cardinality = index.getCardinality();
		int word_length = index.getWordLength();
//		Closest_node node = ApproximitySearch(index,ts,start, stop,false,null);
			 Hashtable<Integer, Object> PAA = Transformations.piecewiseAggregateApproximation(ts, name,start,stop,cardinality);
		double min_dist=-1; 
	
	Closest_node node =ApproximateSearch(index,ts,start,stop,true,PAA);
	return node.getDist();

	
}

} 
