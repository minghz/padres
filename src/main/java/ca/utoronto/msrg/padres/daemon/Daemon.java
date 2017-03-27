package ca.utoronto.msrg.padres.daemon;

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
		
		g.addNode("b1", "localhost/b1");
		g.addNode("b2", "localhost/b2");
		g.addNode("b3", "localhost/b3");
		g.addNode("b4", "localhost/b4");
		g.addNode("b5", "localhost/b5");
		g.addNode("b6", "localhost/b6");
		g.addNode("b7", "localhost/b7");
		g.addNode("b8", "localhost/b8");
		g.addNode("b9", "localhost/b9");
		g.addNode("b10", "localhost/b10");
		
		g.print_graph();
		
		System.out.println("======================================");
		
		g.removeNode("b5");
		g.removeNode("b4");
		g.removeNode("b3");

		g.print_graph();

	
	}
}