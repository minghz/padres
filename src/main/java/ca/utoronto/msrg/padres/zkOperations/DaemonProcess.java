package ca.utoronto.msrg.padres.daemon;

import java.util.Iterator;

import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;


public class DaemonProcess implements Watcher/*, AsyncCallback.StatCallback*/{
	
	String LOCKPATH = "/update_lock";
	ZooKeeper zk;
	ZooKeeperConnection conn;
	MainGraph MG;

	    HashSet<String> currentBrokers = new HashSet<String>();
	    String newBroker = new String();
	
		public DaemonProcess(int maxHops) {
			MG = new MainGraph(maxHops);
			try {
				conn = new ZooKeeperConnection();
				zk = new ZooKeeper("localhost", 5000,this);   
				zk = conn.connect("localhost");
			} catch (Exception e) {
				System.out.println("message"+e.getMessage()); //Catch error message
				System.out.println(e);
				System.exit(1);
			}
		}
	    	
	    public void process(WatchedEvent event) {
	        String path = event.getPath();
	        
	        System.out.println("Current Path:"+path);
	        
	        String rootPath="/";

			if (event.getType() == Event.EventType.None) {
				if (event.getState() == Event.KeeperState.SyncConnected) {
					createUpdateLock();

					try {
						// initialize any existing brokers
						List<String> childrenList = zk.getChildren(rootPath, false);
						for (String child : childrenList) {
							String uri = null;
							byte[] bytes = zk.getData(rootPath + child, false, null);
							if (bytes == null) {
								System.out.println("[error] initialize getData data null! - path = " +
										rootPath + child);
								System.exit(1);
							}
							uri = new String(bytes);
							MG.addNode(child, uri);
						}
					} catch (KeeperException e) {
						System.out.println("[error] initialize getData - path = " +
								e.getPath() + " code = " + e.getCode() + " e: " + e);
						System.exit(1);
					} catch (InterruptedException e) {
						System.out.println("[error] initialize getData - e = " + e);
						System.exit(1);
					}

					updateZK();

					releaseUpdateLock();
				} else {
					System.out.println("weird event??? state = " + event.getState() + " - " + event);
					System.exit(1);
				}
	        } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
		    } else if (event.getType() == Event.EventType.NodeDeleted) {
		        System.out.println("Node Delete Event: " + path);

				assert(path.endsWith("alive"));

				acquireUpdateLock();

				// update graph
				MainGraph newMG = new MainGraph(MG.max_hop);
				for (Node n : MG.graph.getEachNode()) {
					String name = n.getAttribute("name");
					String uri = n.getAttribute("uri");

					// skip removed broker ( /(1) <name> /alive(-6) )
					if (name.equals(path.substring(1, path.length() - 6)))
						continue;

					newMG.addNode(name, uri);
				}
				MG = newMG;

				updateZK();

				releaseUpdateLock();
			}
		}

		// must be called with zookeeper update lock
		private void updateZK() {
			try {
				for (Node broker : MG.graph.getEachNode()) {
					String bidPath = "/" + broker.getAttribute("name");

					// clean existing children
					List<String> children = zk.getChildren(bidPath, false);
					for (String child : children) {
						if (child.equals("alive"))
							continue;

						String childPath = bidPath + "/" + child;
						Stat bidStat = zk.exists(childPath, false);
						zk.delete(childPath, bidStat.getVersion());
						System.out.println("deleted old neighbour path = " + childPath);
					}

					// add new children
					Iterator<Node> neighboursIter = broker.getNeighborNodeIterator();
					while(neighboursIter.hasNext()) {
						Node neighbour = neighboursIter.next();
						String neighbourPath = bidPath + "/" + neighbour.getAttribute("name");
						String neighbourUri = neighbour.getAttribute("uri");
						zk.create(neighbourPath, neighbourUri.getBytes(),
								ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						System.out.println("created neighbour path = " +
								neighbourPath + " data = " + neighbourUri);
					}
				}
			} catch (KeeperException e) {
				System.out.println("[error] updateZK - path = " + e.getPath() +
						" code = " + e.getCode() + " e: " + e);
				System.exit(1);
			} catch (InterruptedException e) {
				System.out.println("[error] updateZK - e = " + e);
				System.exit(1);
			}
		}

		// TODO: ensure only 1 daemon running (and allow for daemon to be
		// restarted) with PID node of currently running daemon
		private void createUpdateLock() {
			try {
				byte[] b = { (byte) 1 };
				Stat lockStat = zk.exists(LOCKPATH, false);
				if (lockStat != null) {
					System.out.println("[warn] lock already exists make sure no other daemons are running...");
					zk.setData(LOCKPATH, b, lockStat.getVersion());
				} else {
					zk.create(LOCKPATH, b, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
			} catch (KeeperException e) {
				System.out.println("[error] createUpdateLock - path = " + e.getPath() +
						" code = " + e.getCode() + " e: " + e);
				System.exit(1);
			} catch (InterruptedException e) {
				System.out.println("[error] createUpdateLock - e = " + e);
				System.exit(1);
			}
		}

		private void acquireUpdateLock() {
			try {
				Stat lockStat = null; 
				byte[] lock = zk.getData(LOCKPATH, false, lockStat);
				// should not be possible to already be in an update...
				assert((int)lock[0] == 0);
				lock[0] = (byte) 1;
				zk.setData(LOCKPATH, lock, lockStat.getVersion());
			} catch (KeeperException e) {
				System.out.println("[error] acquireUpdateLock - path = " + e.getPath() +
						" code = " + e.getCode() + " e: " + e);
				System.exit(1);
			} catch (InterruptedException e) {
				System.out.println("[error] acquireUpdateLock - e = " + e);
				System.exit(1);
			}
		}

		private void releaseUpdateLock() {
			try {
				Stat lockStat = null; 
				byte[] lock = zk.getData(LOCKPATH, false, lockStat);
				// should not be possible to already be out of an update...
				assert((int)lock[0] == 1);
				lock[0] = (byte) 0;
				zk.setData(LOCKPATH, lock, lockStat.getVersion());
			} catch (KeeperException e) {
				System.out.println("[error] releaseUpdateLock - path = " + e.getPath() +
						" code = " + e.getCode() + " e: " + e);
				System.exit(1);
			} catch (InterruptedException e) {
				System.out.println("[error] releaseUpdateLock - e = " + e);
				System.exit(1);
			}
		}

		public static void main(String[] args){

			if (args.length != 1) {
				System.out.println("usage: daemonprocess <max hops>");
			}

			int max_hop = Integer.parseInt(args[0]);
			DaemonProcess dp = new DaemonProcess(max_hop);

			// just sleep whenever zkwatcher isn't working
			while (true) {
				try {
					Thread.sleep(1000000);
				} catch (Exception e) {
					System.out.println("message"+e.getMessage()); //Catch error message
				}
			}
		}


	    public static void createNewGraph(int numofNodes){
	
		MainGraph MG=new MainGraph();

		String path; // Assign path to znode 
	 	byte[] data; // Declare data
   
		      try {
		
		    	 ZKCreate ZKObj = new ZKCreate();
		       
		         for(int i=0;i<numofNodes;i++){
		        	 
		        	 path="/zkOperations"+Integer.toString(i);
		        	 data="My zookeeper node".getBytes();
		        	 
		        	
		        	 ZKObj.create(path, data); 
		        	 
		        	 System.out.println("Create "+i+" node");
		        	 	        	 
		        	 MG.addNode(path, data.toString());
		         }
		         
		         
		         MG.print_graph();
		         
		      } catch (Exception e) {
			         System.out.println("message"+e.getMessage()); //Catch error message
			         System.out.println("message"+e.toString()); //Catch error message
		      }
			
	    }

}
