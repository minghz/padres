package ca.utoronto.msrg.padres.daemon;


import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;

public class ZKDelete {
   private static ZooKeeper zk;
   private static ZooKeeperConnection conn;

   public ZKDelete(){
	   try{
	   conn = new ZooKeeperConnection();
       zk = new ZooKeeper("localhost", 5000,new ZKEventCatcher());   
       zk = conn.connect("localhost");
	   }catch (Exception e) {
	         System.out.println("message"+e.getMessage()); //Catch error message
	   }
	  
   }
   
   // Method to check existence of znode and its status, if znode is available.
   public static void delete(String path) throws KeeperException,InterruptedException {
      zk.delete(path,zk.exists(path,true).getVersion());
   }
/*
   public static void main(String[] args) throws InterruptedException,KeeperException {
      String path = "/zkOperations"; //Assign path to the znode
		
      try {
         conn = new ZooKeeperConnection();
         zk = new ZooKeeper("localhost", 5000,new ZKEventCatcher());
         zk = conn.connect("localhost");
         delete(path); //delete the node with the specified path
      } catch(Exception e) {
         System.out.println(e.getMessage()); // catches error messages
      }
   }*/
}