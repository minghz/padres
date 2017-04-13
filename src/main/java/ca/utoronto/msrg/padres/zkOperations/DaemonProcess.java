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
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.apache.log4j.BasicConfigurator;


public class DaemonProcess implements Watcher/*, AsyncCallback.StatCallback*/{
	
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
	    	
	    static boolean flagDelete=false;
	    
	    public void process(WatchedEvent event) {
	    	
	    	
	        String path = event.getPath();
	        
	        System.out.println();
	        System.out.println("Current Path:"+path);
	    //    System.out.println(event.getType().toString());
	     //   System.out.println();
	        
	        
	        String rootPath="/";
	        
	        
	        
	        if (event.getType() == Event.EventType.None) {
	      //  System.out.println("None Event");
	        	
	            switch (event.getState()) {
	                case SyncConnected:
	                    break;
	                case Expired:
	                    break;
	            }
	            
	            try{
	            	
	            	List<String> childrenList = new ArrayList<String>();
	            	if(path!=null)
	            	childrenList=zk.getChildren(path,this);
	            	else
	            	childrenList=zk.getChildren(rootPath,this);
	            	
	            	int i=0;
	            	
	            	for(Iterator iterator=childrenList.iterator();iterator.hasNext();){	 
	            		i++;
	            		String str=iterator.next().toString();
	            		System.out.println("i="+i+" Children:"+str);
	            		if(path==null){
	            			System.out.println("/"+str);
		            		zk.getChildren("/"+str, this);
		            		break;
		            	}
		            	else if(path.equals("/")){
		            		System.out.println(path+str);
		            		zk.getChildren(path+str, this);
		            		
		            		
		            	}
		            	else{
		            		System.out.println(path+"/"+str);
		            		zk.getChildren(path+"/"+str, this);	
		            		
		            		
		            	}
	            	}
	            	
	            	if(!path.isEmpty()){
	            		zk.getChildren(path, this);
	            	}
	                zk.getChildren(rootPath,this);
	                
	               
	            }catch(Exception e){
	                	System.out.println(e.getMessage());
	                	
	            }    
	        } 
	        
	        
	        else if (event.getType() == Event.EventType.NodeChildrenChanged){
	        	
				List<String> childrenList = zk.getChildren(path,this);
				HashSet<String> childrenSet = new HashSet<String>(childrenList);
				Collections.sort(childrenList);
	       	
	        	if(flagDelete==false){
	        		
	        		//step 1: ZKget
	        		
	        		//step 2: get the whole graph get_graph()
	        		
	        		//ZKCreate("")
	        		
	        		
	        		//Codes if Children is created not deleted
				    // System.out.println("Node Children Changed Event");
				        
				        try{
				        	
				        List<String> childrenList = new ArrayList<String>();
				        
				        
				        if(path!=null)
			            	childrenList = zk.getChildren(path,this);
			            else
			            	childrenList = zk.getChildren(rootPath,this);
				        
				        
				        int i = 0;
		            	
				        String str;
				        
		            	for(Iterator iterator = childrenList.iterator();iterator.hasNext();i++){	 
		            		
		            		str=iterator.next().toString();
		            		
		            	//	System.out.println("i="+i+" Children:"+str);
		            		if(path==null){
		            			System.out.println(rootPath+str);
			            		zk.getChildren(rootPath,this);
			            		
			            		break;
			            	}
			            	else{
			            		if(path.equals("/")){
				            		System.out.println(path+str);
				            		zk.getChildren(path+str, this);
				            		
				            	}
				            	else{
				            		System.out.println(path+"/"+str);
				            		zk.getChildren(path+"/"+str, this);	
				            		
				            	}
			            		
			            		
			            		if(!currentBrokers.contains(str)){
			            			currentBrokers.add(str);
			            			newBroker=str;			          			
			            		}  		
			            	}	
		            	}
		            	
	            		
	            		System.out.println("new Broker is "+ newBroker);
		            	
		            	//Add watcher to current path
		            	if(!path.isEmpty()){
		            		zk.getChildren(path, this);
		            	}
		            	
				        }catch(Exception e){
				        	
				        	System.out.println(e.getMessage());
				        	
				        }
	        	}
	        	else{
	        		flagDelete=false;
	        	
	        	}
       		 
		    }
	        
	        else if (event.getType() == Event.EventType.NodeDeleted) {
		        System.out.println("Node Delete Event: " + path);

				assert(path.endsWith("alive"));

				// update graph
				MainGraph newMG = new MainGraph(MG.max_hop);
				for (Node n : MG.graph.getEachNode()) {
					String name = n.getAttribute("name");
					String uri = n.getAttribute("uri");

					// skip removed broker
					if (name.equals(path.substring(0, path.length() - 6))
						continue;

					newMG.addNode(name, uri);
				}
				MG = newMG;

				// update zookeeper
		    }
	    }

		public static void main(String[] args){

			if (args.len != 1) {
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
