package Graph_Algorithm;

import java.util.ArrayList;

import graphComponent.*;

public class NodeExtendedBFS extends Node{

	public static enum Color {
	    WHITE, GRAY, BLACK
	};

	private Color color;
	private int distance;
	
	public NodeExtendedBFS(String nodeStr) {
		super();
		parseinputRow(nodeStr);
	}
	
    public NodeExtendedBFS(Integer id) {
	    super.id = id;
    }
	
	public void parseinputRow(String nodeStr)
	{
		String[] tokens = nodeStr.split("\t");
		try{
			setId(Integer.parseInt(tokens[0]));
			String[] intermediate = tokens[1].split("\\|");
			String[] dstNodes = intermediate[0].split(",");
			String distance = intermediate[1];
			ArrayList<Integer> edgeList = new ArrayList();
			for(String node : dstNodes)
			{
				if(!node.equals("NULL"))
				edgeList.add(Integer.parseInt(node));
			}
			setEdgeList(edgeList);
			//if(distance.equals("Integer.MAX_VALUE"))
			if(distance.equals("Integer.MAX"))
			{
				setDistance(Integer.MAX_VALUE);	
			}else{
				setDistance(Integer.parseInt(distance));
			}
			setColor(Color.valueOf(intermediate[2]));
		}
		catch(Exception e)
		{
			System.out.println("Not proper Input, override process method");
		}
	}
	
	public String toString()
	{
		String output = "";
		output = super.toString();
		return (output += Integer.toString(distance)+ "|" + color);
	}
	
	public String createOutputRow(ArrayList<Integer> edgeList, ArrayList<Float> weightList, String color)
	{
		String output = "";
		output = super.createOutputRow(edgeList, weightList);
		return (output += Integer.toString(distance)+ "|" + color);
	}
	
	public void setColor(Color color)
	{
		this.color = color;
	}
	
	public Color getColor()
	{
		return color;
	}
	
	public void setDistance(int distance)
	{
		this.distance = distance;
	}
	
	public int getDistance()
	{
		return distance;
	}

}
