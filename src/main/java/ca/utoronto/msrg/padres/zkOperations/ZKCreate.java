package ca.utoronto.msrg.padres.zkOperations;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import org.apache.log4j.BasicConfigurator;


import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;


public class ZKCreate {
   // create static instance for zookeeper class.
   private static ZooKeeper zk;

   // create static instance for ZooKeeperConnection class.
   private static ZooKeeperConnection conn;
 

   public static void main(String[] args) {	   
	   
	   BasicConfigurator.configure();
	  
    
     String path = "/zkOperations"; // Assign path to znode
  
     
      // data in byte array
      byte[] data = "My first zookeeper app".getBytes(); // Declare data
      
      		
      try {
    	  
         conn = new ZooKeeperConnection();
         zk = new ZooKeeper("localhost", 5000,new ZKEventCatcher());
         
         zk = conn.connect("localhost");
       
         create(path, data);
         conn.close();
         
      } catch (Exception e) {
         System.out.println("message"+e.getMessage()); //Catch error message
      }
   }
   
   
  
   // Method to create znode in zookeeper ensemble
   public static void create(String path, byte[] data) throws 
      KeeperException,InterruptedException {
      zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT);
      
   }

   
}