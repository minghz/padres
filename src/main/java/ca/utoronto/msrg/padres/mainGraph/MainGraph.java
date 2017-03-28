package ca.utoronto.msrg.padres.daemon;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;

import org.graphstream.graph.*;

import org.graphstream.graph.implementations.*;

import org.graphstream.stream.Source.*;
//import org.graphstream.stream.Source.Generator;
import org.graphstream.algorithm.generator.*;

public class MainGraph
{
	// note: how to run on comand line:
	//
	//> cd src/main/java/
	//> java -cp ../../../target/padres-broker-jar-with-dependencies.jar:../../../target/classes/ ca.utoronto.msrg.padres.graph.MainGraph

	/*================ object parameters =================*/

	private Graph graph;
	Random rnd;
	
	/*================ constructor =================*/

	public MainGraph (){
		this.graph = new SingleGraph("MainGraph");
		this.graph.display();
		this.rnd = new Random();
	}
	
	/*================ public functions =================*/

	public void addNode(String node_name, String node_uri){
		
		int node_count = this.graph.getNodeCount();
		System.out.println(node_count);
		
		Node node = create_node(node_name, node_uri);
		
		if (node_count == 0){
			System.out.println("zero---");
			print_node(node);
			
		} else if (node_count == 1){
			System.out.println("one---");
			print_node(node);
			
			connect_to_all_other_nodes(node);
						
		} else if (node_count == 2) {
			System.out.println("two---");
			print_node(node);
			
			connect_to_all_other_nodes(node);

		} else {
			System.out.println("three---");

			int edge_count = this.graph.getEdgeCount();
			int stop_i = this.rnd.nextInt(edge_count);
			System.out.println(stop_i);

			int i = 0;
			for(Edge e:this.graph.getEachEdge()) {
				if (i >= stop_i) {
				
					System.out.println(e.getNode1() +"--"+ e.getNode0());
					
					System.out.println(node.getId()+"->"+e.getNode0().getId());
					this.graph.addEdge(node.getId()+e.getNode0().getId(), node.getId(), e.getNode0().getId());
					System.out.println(node.getId()+"->"+e.getNode1().getId());
					this.graph.addEdge(node.getId()+e.getNode1().getId(), node.getId(), e.getNode1().getId());
					
					break;			
				}
				i++;
			}
		}
		
		node_count = this.graph.getNodeCount();
		System.out.println(node_count);
	}
	
	public void removeNode(String node_name){
		for(Node n:this.graph.getEachNode()) {
			if(n.getAttribute("name").toString() == node_name){
				this.graph.removeNode(n);
			}
		}
	}
	
	public void print_graph() {		
		//Generator gen = new DorogovtsevMendesGenerator();
		//gen.addSink(this.graph);
		//gen.begin();
		//for(int i = 0; i < graph_size; i++) {
		//	gen.nextEvents();
			/*//sleep for a little bit so it animates right
			try {
			    Thread.sleep(500);
			} catch(InterruptedException ex) {
			    Thread.currentThread().interrupt();
			}
			// end sleep*/
		//}
		//gen.end();
		// loop through all generated nodes

		for(Node n:this.graph.getEachNode()) {
			//n.addAttribute("uri", "");	
			//n.addAttribute("name", "b" + n.getId() );				

			System.out.println("id: " + n.getId()); // id of this node
			System.out.println("name: " + n.getAttribute("name").toString()); // id of this node
			System.out.println("uri: " + n.getAttribute("uri").toString()); // id of this node

			Iterator<Node> nodes = n.getNeighborNodeIterator();
			while(nodes.hasNext()) {
				Node node = nodes.next();
				System.out.println("-- id: " + node.getId()); // id of this node
				System.out.println("-- name: " + node.getAttribute("name").toString()); // id of this node
				System.out.println("-- uri: " + node.getAttribute("uri").toString()); // id of this node
			}
			System.out.println("-------------------");
		}		
	}
	
	
	
	//	{
	//		{nodeId} => {
	//			"name" => <string>,
	//			"uri"  => <string>,
	//			"neighbours" => "b3, b2"
	//		}, .. 
	//	}
	public Hashtable<String, Hashtable<String, String>> get_graph() {
		
		Hashtable<String, Hashtable<String, String>> result = new Hashtable<String, Hashtable<String, String>>();
		
		for(Node n:this.graph.getEachNode()) {
			
			Hashtable<String, String> details = new Hashtable<String, String>();

			details.put("name", n.getAttribute("name").toString());
			details.put("uri", n.getAttribute("uri").toString());
			details.put("neighbours", get_node_neighbours(n));
			
			result.put(n.getId(), details);
		}
		
		return result;
	}
	
	
	/*================ private functions =================*/
	
	private String get_node_neighbours(Node n) {
		String result = "";
		Iterator<Node> nodes = n.getNeighborNodeIterator();
		while(nodes.hasNext()) {
			Node node = nodes.next();
			if (result.length() == 0) {
				result = result + node.getId();
			} else {
				result = result + ", " + node.getId();
			}
		}
		return result;
	}
	private Node create_node(String name, String uri){
		Node node = this.graph.addNode(name);
		node.addAttribute("name", name);
		node.addAttribute("uri", uri);
		
		return node;
	}
	private void print_node(Node node){
		System.out.println(node.getId());
		System.out.println(node.getAttribute("name").toString());
		System.out.println(node.getAttribute("uri").toString());
	}
	private void connect_to_all_other_nodes(Node node){
		for(Node n:this.graph.getEachNode()){
			if(! n.hasEdgeBetween(node) && n != node){
				this.graph.addEdge(	node.getAttribute("name").toString() + n.getId(),
								   	node.getAttribute("name").toString(),
								   	n.getId());
			}
		}
	}
	
}