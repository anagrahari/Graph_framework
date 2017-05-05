package graphComponent;

import java.io.BufferedReader;

import com.google.common.base.Joiner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Node {
	protected Integer id;
	protected ArrayList<Integer> edgeList;
	protected ArrayList<Float> weightList;
	
    private static final Joiner NEW_LINE = Joiner.on("\n");

    /**
     * Defines the starting point for BFS algorithm
     */
    private final static int SOURCE_VERTEX = 0;
	
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
	
	public static void convert(String path) throws IOException {
		 	String problemFile = path + "/sparkinput";
	        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(problemFile))));
	       
	        int vertexCount = Integer.parseInt(reader.readLine());
	        Map<Integer, Vertex> vertices = new HashMap<>(vertexCount);
//	        System.out.println("Vertex Count: " + vertexCount);
	        String line = reader.readLine(); // number of edges [unused]
	        String []next  = reader.readLine().split(" ");
//	        System.out.println("First Line:" + next);
	        int source = Integer.parseInt(next[0]);
	        int neighbor = Integer.parseInt(next[1]);
	        
	        vertices.put(source, new Vertex(source, new HashSet<Integer>(), 0, Color.GRAY));
	        vertices.get(source).addNeighbour(neighbor);
//	        for (Vertex value : vertices.values()){
//	        	System.out.println(value.toString());
//	        }
	        
	        for (int i = 1; i < vertexCount; i++) {
	        	if(!vertices.containsKey(i))
	        		vertices.put(i, new Vertex(i, new HashSet<Integer>(), Integer.MAX_VALUE, Color.WHITE));
	        }
	        
//	        System.out.println("Edges: " + line);
	        
	        while ((line = reader.readLine()) != null) {
//	        	System.out.println(line);
	        	String[] pair  = line.split(" ");
	            int vertex1 = Integer.parseInt(pair[0]);
	            int vertex2 = Integer.parseInt(pair[1]);
	            if (vertices.containsKey(vertex1) && vertices.containsKey(vertex2)) {
	            	vertices.get(vertex1).addNeighbour(vertex2);
	            	vertices.get(vertex2).addNeighbour(vertex1);
	            }
	        }
//	        for (Vertex value : vertices.values()){
//	        	System.out.println(value.toString());
//	        }
	        
//	       System.out.println(NEW_LINE.join(vertices.values()));
	        
	        try (PrintStream out = new PrintStream(new FileOutputStream("filename.txt"))) {
	        	String temp = NEW_LINE.join(vertices.values());
	        	temp = temp + "\n";
	            out.print(temp);
	        }
	        
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
	        Path localPath = new Path("filename.txt");
	        Path hdfsPath = new Path(path+"/bfs");
	        fs.copyFromLocalFile(localPath, hdfsPath);
	        
//	        Files.write(Paths.get(problemFile + "_0"), NEW_LINE.join(vertices.values()).getBytes(), StandardOpenOption.CREATE);
	    }
}
