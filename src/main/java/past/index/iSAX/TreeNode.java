package past.index.iSAX;

import com.typesafe.config.Config; 
import com.typesafe.config.ConfigFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import past.Transformations;
import past.Timeseries;
import java.util.*;

public class TreeNode{

//struct TS_info{
//	Path original_path;
//	Path sax_path;
//}

public static enum NodeType {ROOT,INTERNAL,TERMINAL};
//node type
public NodeType myType;

//last_prometed index if internal
public int last_index_promoted=1;
//base cardinality, used to know when the split happened
private int base_card;

//current cardinality of the node
private int cardinality;

private int word_len;
//descendents of the node
public TreeMap<String,TreeNode> children;
//map of timeseries indexed by this node, saved for terminal nodes
public HashMap<String,Path> indexed_timeseries;

//threshold which determines when to split
private int th;

public static int th_max;
private Path dataPath;  

public int getCardinality(){
	return cardinality;
}
public TreeNode(int cardinality, int word_len,NodeType t,int base_cardinality){
	children=new TreeMap<String,TreeNode>();	
	this.myType=t;
	this.word_len=word_len;
	this.cardinality=cardinality;
	this.base_card=base_cardinality;
	indexed_timeseries=new HashMap<String,Path>();
	
	this.th=0;
	
}






protected String getSAXStringRep(String ts, int new_card,char c){
	StringBuilder builder =new StringBuilder(ts);
	   // System.out.println(log2(cardinality));
	builder.insert(iSAX_dist_utils.log2(cardinality), c);
	
	return builder.toString();
}
public boolean insert(String SAX , String name, Path path,int card){
System.out.println("..............Indexing" + name + "::"+SAX);
	if(children.containsKey(SAX)){
	System.out.println("..............Contains the key");
		TreeNode node=children.get(SAX);
		if(node.myType == NodeType.TERMINAL){
			if(node.indexed_timeseries.size()==TreeNode.th_max){
				//split
					System.out.println("..............SPLIT");
				TreeNode newInternal=new TreeNode(cardinality,word_len,NodeType.INTERNAL,base_card);
				
				
				
				
				node.indexed_timeseries.put(name,path);
				children.remove(SAX);
				children.put(SAX, newInternal);
				
				for (Map.Entry<String, Path> cursor : node.indexed_timeseries.entrySet())
					  insert(SAX,cursor.getKey(),cursor.getValue(),cardinality);				
				
				return true;
			}
			else {
					System.out.println(".............NO SPLIT");
				children.get(SAX).indexed_timeseries.put(name,path);
				children.get(SAX).th++;
				return true;
			}
			
		}
		else{
			System.out.println("..............CONTAINS THE KEY BUT IT'S AN INTERNAL NODE");
		 	String newSAX=getSAXStringRep(SAX,cardinality,last_index_promoted==0?'1':'0');
			last_index_promoted=1-last_index_promoted;
		 	return node.insert(newSAX,name,path,cardinality*2);
		}
	
		
	}
	else{
		System.out.println(".............DOESNT CONTAINT THE KEY");
		TreeNode newNode=new TreeNode(card, word_len, NodeType.TERMINAL, base_card);
		newNode.indexed_timeseries.put(name,path);
		newNode.th++;
		children.put(SAX,newNode);
		return true;
	}




}


}
