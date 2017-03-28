package ca.utoronto.msrg.padres.daemon;

import java.util.Hashtable;

public class Daemon
{
	// note: how to run on comand line:
	//
	//> cd src/main/java/
	//> java -cp ../../../target/padres-broker-jar-with-dependencies.jar:../../../target/classes/ ca.utoronto.msrg.padres.daemon.Daemon
	
	public static void main(String args[]) {
			
		// first argument args[1] is a number i.e. 10		
		int graph_size = Integer.parseInt(args[0]);
		
		MainGraph g = new MainGraph();
		
		for(Integer i=1; i <= graph_size; i++){
			g.addNode("b"+ i.toString(), "localhost/b"+i.toString());
		}
		
		g.print_graph();
		
		System.out.println("======================================");
		
		Hashtable<String, Hashtable<String, String>> complete_graph = g.get_graph();
		System.out.println( complete_graph );
		System.out.println( "Key Set: " + complete_graph.keySet() );
		
		//g.removeNode("b5");
		//g.removeNode("b4");
		//g.removeNode("b3");

		//g.print_graph();

	
	}
}