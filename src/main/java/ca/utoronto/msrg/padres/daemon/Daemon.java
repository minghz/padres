package ca.utoronto.msrg.padres.daemon;

import org.graphstream.graph.*;
import org.graphstream.graph.implementations.*;

public class Daemon
{
	// note: how to run on comand line:
	//
	//> cd src/main/java/
	//> java -cp ../../../target/padres-broker-jar-with-dependencies.jar:../../../target/classes/ ca.utoronto.msrg.padres.daemon.Daemon
	
		public static void main(String args[]) {
			Graph graph = new SingleGraph("Tutorial 1");

			graph.addNode("Apples");
			graph.addNode("Bees");
			graph.addNode("Cu");
			graph.addEdge("ApplesBees", "Apples", "Bees");
			graph.addEdge("BeesCu", "Bees", "Cu");
			graph.addEdge("CuApples", "Cu", "Apples");

			graph.display();
		}
}