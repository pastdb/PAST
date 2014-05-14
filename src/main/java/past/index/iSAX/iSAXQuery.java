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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import com.typesafe.config.Config; 
import com.typesafe.config.ConfigFactory;

import java.net.URI;
import java.net.URISyntaxException;

public class iSAXQuery{

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
public static HashMap<String,Path> ApproximateSearch(iSAX_Index index, Timeseries ts,int start, int stop){
		
		int cardinality = index.getCardinality();
		int word_length = index.getWordLength();
				System.out.println("card::"+cardinality+"::word::"+word_length+"::name::"+ts.getPath().getName()+"::start::"+start+"::stop::"+stop);
		Hashtable<Integer,Point> SAX = Transformations.symbolicAggregateApproximation(ts, ts.getPath().getName(), start, stop, word_length, cardinality);		
		String saxWord=iSAX_Index.getSAXString(SAX);	
		System.out.println("Searching for "+saxWord);
		TreeNode node=iSAXQuery.traverseTree(index.getRoot(),saxWord);
		if(null!=node){
		
			if(node.myType==TreeNode.NodeType.TERMINAL)
				return node.indexed_timeseries;
				
			else{
			
				TreeNode childNode=null;
			while(true){
			
				String childSAXRep= node.getSAXStringRep(saxWord, cardinality*2 ,'0');
					childNode=traverseTree(node,childSAXRep);
				if(null!=childNode && childNode.myType==TreeNode.NodeType.TERMINAL)
					return childNode.indexed_timeseries;
				if(null==childNode){
					childSAXRep=node.getSAXStringRep(saxWord,cardinality*2,'1');
					childNode=traverseTree(node,childSAXRep);
					if(null!=childNode && childNode.myType==TreeNode.NodeType.TERMINAL)
					return childNode.indexed_timeseries;
				}
				if(null==childNode){
					break;
				}
			}
			}	
			
		}
		else {
			
		}
		return null;	
}

}
