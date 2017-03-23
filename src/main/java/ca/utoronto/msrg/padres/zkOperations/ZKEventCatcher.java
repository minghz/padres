package ca.utoronto.msrg.padres.zkOperations;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.*;


public class ZKEventCatcher  implements Watcher, AsyncCallback.StatCallback{
	
	
	  String path = "/zkOperations";
	    ZooKeeper zk=null;
	    Object o =new Object();

	    public void init(){
	        try {
	            zk = new ZooKeeper("localhost",5000,this);
	            zk.exists(path,this);
	            synchronized (o){
	                o.wait();
	            }
	        } catch (IOException e) {
	            e.printStackTrace();
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	    }

	    public void processResult(int i, String s, Object o, Stat stat) {
	        boolean exists;
	        switch (KeeperException.Code.get(i)) {
	            case OK:
	                exists = true;
	                break;
	            case NONODE:
	                exists = false;
	                break;
	            case SESSIONEXPIRED:
	            case NOAUTH:
	                return;
	            default:
	                zk.exists(s, true, this, stat);
	                return;
	        }
	        if(exists){
	            try {
	                String val = new String(zk.getData(path,false,null));
	                System.out.println(val);
	            } catch (KeeperException e) {
	                e.printStackTrace();
	            } catch (InterruptedException e) {
	                e.printStackTrace();
	            }
	        }
	    }

	    public void process(WatchedEvent event) {
	        String path = event.getPath();
	        System.out.println();
	        System.out.println("CATCH EVENT!!!");
	        System.out.println();
	        if (event.getType() == Event.EventType.None) {
	            switch (event.getState()) {
	                case SyncConnected:
	                    break;
	                case Expired:
	                    break;
	            }
	        } else {
	            if (path != null) {
	                zk.exists(path, true, this, null);
	            }
	        }
	    }

}
