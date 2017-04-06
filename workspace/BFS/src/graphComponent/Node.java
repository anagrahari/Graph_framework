package graphComponent;

import java.util.ArrayList;
import java.util.Iterator;

public class Node {
	protected Integer id;
	protected ArrayList<Integer> edgeList;
	protected ArrayList<Float> weightList;
	
	public Node(){
		
	}
	public Node(String nodeStr)
	{
		parseinputRow(nodeStr);
	}

	/*It will process edge in following string representation 
	Id\tdstNode1,dstNode2|weight1,weight2
	User need to extend this class and override this method in case of a different input format
	*/
	public void parseinputRow(String nodeStr)
	{
		String[] tokens = nodeStr.split("\t");
		try{
			setId(Integer.parseInt(tokens[0]));
			String[] intermediate = tokens[1].split("\\||");
			String[] dstNodes = intermediate[0].split(",");
			String[] weights = intermediate[1].split(",");
			for(String node : dstNodes)
			{
				getEdgeList().add(Integer.parseInt(node));
			}
			for(String weight : weights)
			{
				getWeightList().add(Float.parseFloat(weight));
			}
		}
		catch(Exception e)
		{
			System.out.println("Not proper Input, override process method");
		}
	}
	
	public String toString()
	{
		String output = "";
		if(edgeList != null){
			Iterator<Integer> itrEdge =  edgeList.iterator();
			while(itrEdge.hasNext())
			{
				output += Integer.toString(itrEdge.next());
				if(itrEdge.hasNext())
					output += ",";
			}
		}else{
			output += "NULL";
		}
		output += "|";
		if(weightList != null)
		{	
			Iterator<Float> itrWeight =  weightList.iterator();
			while(itrWeight.hasNext())
			{
				output += Float.toString(itrWeight.next());
				if(itrWeight.hasNext())
					output += ",";
			}
			output += "|";
		}
		return output;
	}
	
	public String createOutputRow(ArrayList<Integer> edgeList, ArrayList<Float> weightList)
	{
		String output = "";
		if(edgeList != null){
			Iterator<Integer> itrEdge =  edgeList.iterator();
			while(itrEdge.hasNext())
			{
				output += Integer.toString(itrEdge.next());
				if(itrEdge.hasNext())
					output += ",";
			}
		}else{
			output += "NULL";
		}
		output += "|";
		if(weightList != null)
		{	
			Iterator<Float> itrWeight =  weightList.iterator();
			while(itrWeight.hasNext())
			{
				output += Float.toString(itrWeight.next());
				if(itrWeight.hasNext())
					output += ",";
			}
			output += "|";
		}
		return output;
	}
	
	public void setId(Integer id)
	{
		this.id = id;
	}
	
	public Integer getId()
	{
		return id;
	}
	
	public void setEdgeList(ArrayList<Integer> edgeList)
	{
		this.edgeList = edgeList;
	}
	
	public ArrayList<Integer> getEdgeList()
	{
		return edgeList;
	}
	
	public void setWeightList(ArrayList<Float> weightList)
	{
		this.weightList = weightList;
	}
	
	public ArrayList<Float> getWeightList()
	{
		return weightList;
	}
}
