package ca.utoronto.msrg.padres.daemon;

import org.graphstream.graph.*;
import org.graphstream.graph.implementations.*;

public class Daemon
{
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