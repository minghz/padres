package ca.utoronto.msrg.padres.daemon;

import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;

import org.graphstream.graph.*;

import org.graphstream.graph.implementations.*;

import org.graphstream.stream.Source.*;
//import org.graphstream.stream.Source.Generator;
import org.graphstream.algorithm.generator.*;

public class MainGraph {

	protected static Logger graphLogger = Logger.getLogger(MainGraph.class);
	// note: how to run on comand line:
	//
	//> cd src/main/java/
	//> java -cp ../../../target/padres-broker-jar-with-dependencies.jar:../../../target/classes/ ca.utoronto.msrg.padres.graph.MainGraph <number of nodes> <max hops>

	/*================ object parameters =================*/

	Graph graph;
	Random rnd;
	Integer max_hop; // maximum number of hops allowed in the graph
	Integer cur_hop; // current highest number of hops

	/*================ constructor =================*/

	public MainGraph (){
		this.graph = new SingleGraph("MainGraph");
		this.graph.display();
		this.rnd = new Random();
		this.max_hop = 0;
		this.cur_hop = 0;
	}

	public MainGraph (Integer max_hop){
		this.graph = new SingleGraph("MainGraph");
		//this.graph.display();
		this.rnd = new Random();
		this.max_hop = max_hop;
		this.cur_hop = 0;
	}

	/*================ public functions =================*/

	public void addNode(String node_name, String node_uri) {

		int node_count = this.graph.getNodeCount();
		graphLogger.info("========== Adding node!! " + node_count);

		Node new_n = create_node(node_name, node_uri, this.cur_hop);
		graphLogger.info("created node " + new_n.getId());

		if (node_count == 0){
			//System.out.println("first new_n");
			return;
		}

		boolean added = false;
		while (!added){

			int stop_i = this.rnd.nextInt(node_count);
			int i = 0;

			for(Node n:this.graph.getEachNode()) {
				if (i >= stop_i) { // found random node
					//System.out.println(n.getId() + " " + n.getAttribute("name") + " " + n.getAttribute("uri"));
					graphLogger.info("node_to_add_to " + n.getId() + " " + n.getAttribute("node_dist"));
					Integer node_dist = n.getAttribute("node_dist");

					if (this.max_hop == 0){ // no worries, no max_hop
						this.graph.addEdge(new_n.getId() + n.getId(), new_n, n);
						added = true;
						break;
					}
					else if ( node_dist  < this.max_hop ) {
						//found a node where we can add something to
						//add that node;
						//traverse that node and find out cur_hop
						//if distance is < cur_hop, can update
						//update node_dist for the new node;
						//update node_dist for all nodes in the graph starting from new_n;
						this.graph.addEdge(new_n.getId() + n.getId(), new_n, n);

						//traverse nodes starting from the added node
						Integer longest_path = 0;
						Iterator<Node> neighbours = new_n.getNeighborNodeIterator();
						while(neighbours.hasNext()) {
							Node next_node = neighbours.next();

							graphLogger.info("traverse node " + new_n.getId());

							longest_path = update_node_dist( new_n ,next_node, 1);
						}

						new_n.setAttribute("node_dist", longest_path);
						graphLogger.info("cur_hop " + longest_path);

						graphLogger.info("node dists " + n.getId() + " " + n.getAttribute("node_dist").toString());
						graphLogger.info("node dists " + new_n.getId() + " " + new_n.getAttribute("node_dist").toString());
						added = true;
					}
					break;			
				}
				i++;
			} // end for loop

		} // end while loop

	} // end function


	public void print_graph() {		

		for(Node n:this.graph.getEachNode()) {

			graphLogger.info("id: " + n.getId()); // id of this node
			graphLogger.info("name: " + n.getAttribute("name").toString()); // id of this node
			graphLogger.info("uri: " + n.getAttribute("uri").toString()); // id of this node

			Iterator<Node> nodes = n.getNeighborNodeIterator();
			while(nodes.hasNext()) {
				Node node = nodes.next();
				graphLogger.info("-- id: " + node.getId()); // id of this node
				graphLogger.info("-- name: " + node.getAttribute("name").toString()); // id of this node
				graphLogger.info("-- uri: " + node.getAttribute("uri").toString()); // id of this node
			}
			graphLogger.info("-------------------");
		}		
	}

	// Hashtable format
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
	private Node create_node(String name, String uri, Integer node_dist){
		Node node = this.graph.addNode(name);
		node.addAttribute("name", name);
		node.addAttribute("uri", uri);
		node.addAttribute("node_dist", node_dist); // for max_hop calculation
		node.addAttribute("ui.label", name);

		return node;
	}
	private void print_node(Node node){
		graphLogger.info(node.getId());
		graphLogger.info(node.getAttribute("name").toString());
		graphLogger.info(node.getAttribute("uri").toString());
		graphLogger.info(node.getAttribute("node_dist").toString());
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
	/* Traverse through graph
	 * go through each node starting from the given node
	 * if node traversed to is the last node, update that node's node_dist
	 * */
	private Integer update_node_dist(Node came_from_node, Node node, Integer hops){
		graphLogger.info("update_node_dist " + node.getId() + " hops: " + hops.toString());

		//hops = hops + 1;

		//this is an extremety node
		if(node.getEdgeSet().size() <= 1 ) { 

			graphLogger.info("found extreme node " + node.getId());

			Integer dist = node.getAttribute("node_dist");
			if( dist < hops) {
				node.setAttribute("node_dist", hops);
				if(this.cur_hop < hops){
					this.cur_hop = hops;
				}
				graphLogger.info("set distance for it " + node.getId() + " " + node.getAttribute("node_dist"));
			}

			return hops;

		}

		//this is not an extremety node
		Integer longest_path = hops;
		Iterator<Node> neighbours = node.getNeighborNodeIterator();
		while(neighbours.hasNext()) {
			Node next_node = neighbours.next();
			if(next_node != came_from_node){
				graphLogger.info("traverse node " + next_node.getId());

				Integer path_length = update_node_dist( node, next_node, hops + 1);
				if (path_length > longest_path){
					longest_path = path_length;
				}
			}
		}

		return longest_path;
	}
}



// LEGACY ================================
// //Generator gen = new DorogovtsevMendesGenerator();
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




	/* This function is not being used
	 * 
	 * 
	public void removeNode(String node_name){
		for(Node n:this.graph.getEachNode()) {
			if(n.getAttribute("name").toString() == node_name){
				this.graph.removeNode(n);
			}
		}
	}
	*/
	

	/* Using Dorogovtsev-Mendes algorithm to insert a new node
	 * 
	 */
/*
	public void addNode_old(String node_name, String node_uri){
		
		int node_count = this.graph.getNodeCount();
		System.out.println(node_count);
		
		Node node = create_node(node_name, node_uri, 1);
		
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
	*/

