package ca.utoronto.msrg.padres.daemon;

import org.graphstream.graph.*;
import org.graphstream.graph.implementations.*;

public class Daemon
{
   public static void main(String[] args){
	   System.out.println("Hello World");
	   //System.setProperty("org.graphstream.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer");
	   
	   Graph graph = new SingleGraph("Tutorial 1");
	   
	   graph.addNode("A" );
	   graph.addNode("B" );
	   graph.addNode("C" );
	   graph.addEdge("AB", "A", "B");
	   graph.addEdge("BC", "B", "C");
	   graph.addEdge("CA", "C", "A");
	   
	   graph.display();
	   
	   for(Node n:graph.getEachNode()) {
		   System.out.println(n.getId());
	    }
	   
	    for(Edge e:graph.getEachEdge()) {
	        System.out.println(e.getId());
	    }
   }
}