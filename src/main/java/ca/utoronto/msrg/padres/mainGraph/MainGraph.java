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

	Graph graph;
	Random rnd;
  Integer max_hop; // maximum number of hops allowed in the graph
  Integer cur_hop; // current highest number of hops
	
	/*================ constructor =================*/

  public MainGraph (){
		this.graph = new SingleGraph("MainGraph");
		this.graph.display();
		this.rnd = new Random();
	}

	public MainGraph (Integer max_hop){
		this.graph = new SingleGraph("MainGraph");
		this.graph.display();
		this.rnd = new Random();
    this.max_hop = max_hop;
    this.cur_hop = 0;
	}
	
	/*================ public functions =================*/

	public void addNode(String node_name, String node_uri){
		
		int node_count = this.graph.getNodeCount();
		System.out.println("========== Adding node!! " + node_count);

		Node new_n = create_node(node_name, node_uri, this.cur_hop);
		System.out.println("created node " + new_n.getId());
		
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
          System.out.println("node_to_add_to " + n.getId() + " " + n.getAttribute("node_dist"));
          Integer node_dist = n.getAttribute("node_dist");
          if ( node_dist  < this.max_hop ) {
            //found a node where we can add something to
            //add that node;
            //traverse that node and find out cur_hop
            //if distance is < cur_hop, can update
            //update node_dist for the new node;
            //update node_dist for all nodes in the graph starting from new_n;
            this.graph.addEdge(new_n.getId() + n.getId(), new_n, n);
           
            //traverse nodes starting from the added node
            Iterator<Node> neighbours = new_n.getNeighborNodeIterator();
            while(neighbours.hasNext()) {
              Node next_node = neighbours.next();

              System.out.println("traverse node " + new_n.getId());
              
              update_node_dist( new_n ,next_node, 1);
            }

            new_n.setAttribute("node_dist", this.cur_hop);
            System.out.println("cur_hop " + this.cur_hop);

            System.out.println("node dists " + n.getId() + " " + n.getAttribute("node_dist").toString());
            System.out.println("node dists " + new_n.getId() + " " + new_n.getAttribute("node_dist").toString());
            added = true;
          }
          break;			
        }
        i++;
      } // end for loop

    } // end while loop

	} // end function

	/* Using Dorogovtsev-Mendes algorithm to insert a new node
	 * 
	 */
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
	private Node create_node(String name, String uri, Integer node_dist){
		Node node = this.graph.addNode(name);
		node.addAttribute("name", name);
		node.addAttribute("uri", uri);
		node.addAttribute("node_dist", node_dist); // for max_hop calculation
		
		return node;
	}
	private void print_node(Node node){
		System.out.println(node.getId());
		System.out.println(node.getAttribute("name").toString());
		System.out.println(node.getAttribute("uri").toString());
		System.out.println(node.getAttribute("node_dist").toString());
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
  private void update_node_dist(Node came_from_node, Node node, Integer hops){
    System.out.println("update_node_dist " + node.getId() + " hops: " + hops.toString());

    //hops = hops + 1;
    
    //this is an extremety node
    if(node.getEdgeSet().size() <= 1 ) { 
      
      System.out.println("found extreme node " + node.getId());

      Integer dist = node.getAttribute("node_dist");
      if( dist < hops) {
        node.setAttribute("node_dist", hops);
        if(this.cur_hop < hops){
          this.cur_hop = hops;
        }
        System.out.println("set distance for it " + node.getId() + " " + node.getAttribute("node_dist"));
      }

      return;

    }
    
    //this is not an extremety node
    Iterator<Node> neighbours = node.getNeighborNodeIterator();
		while(neighbours.hasNext()) {
			Node next_node = neighbours.next();
      if(next_node != came_from_node){
        System.out.println("traverse node " + next_node.getId());
        
        update_node_dist( node, next_node, hops + 1);
      }
		}
  }


}
