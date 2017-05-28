package ca.utoronto.msrg.padres.daemon;

import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

import java.util.Iterator;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZKUtil;

import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;


public class DaemonProcess implements Runnable {

	protected static Logger daemonLogger = Logger.getLogger(DaemonProcess.class);

	private static final String ROOT_PATH = "/padres";
	private static final String GLOBAL_FLAG = "global_flag";
	private static final String GLOBAL_FLAG_PATH = ROOT_PATH + "/" + GLOBAL_FLAG;
	private static final String ALIVE = "alive";
	private static final String BROKER_FLAG = "broker_flag";
	private ZooKeeper zk;
	private MainGraph MG;

	public DaemonProcess(int maxHops) {
		MG = new MainGraph(maxHops);
		try {
			// TODO: make this string come from config or something...
			ZKConnect zkc = new ZKConnect("localhost");
			zk = zkc.zk;
		} catch (Exception e) {
			daemonLogger.error("message"+e.getMessage()); //Catch error message
			daemonLogger.error(e);
			System.exit(1);
		}
	}

	private class ZKConnect implements Watcher {
		public ZooKeeper zk;
		private DaemonWatcher dw;

		public ZKConnect(String zkHost) {
			try {
				zk = new ZooKeeper(zkHost, 5000, this);
			} catch (IOException e) {
				daemonLogger.error("Failed to connect to ZooKeeper: " + zkHost + " with: ", e);
				System.exit(1);
			}
		}
		
		public void process(WatchedEvent we) {
			if (we.getState() == Event.KeeperState.SyncConnected) {
				// TODO: ensure only 1 daemon running (and allow for daemon to be
				// restarted) with PID node of currently running daemon
				initZKNode(ROOT_PATH, "");
				initZKNode(GLOBAL_FLAG_PATH, "0");
				try {
					dw = new DaemonWatcher();

					Set<Entry<String, String>> newBrokerSet = dw.getLiveBrokers();
					if (newBrokerSet != null)
						dw.doUpdate(newBrokerSet);
				} catch (KeeperException e) {
					daemonLogger.error("zkconnect trying update - path = " + e.getPath() +
							" code = " + e.getCode() + " e: ", e);
					System.exit(1);
				} catch (InterruptedException e) {
					daemonLogger.error("zkconnect trying update - e = ", e);
					System.exit(1);
				}
			} else {
				daemonLogger.error("weird event??? state = " + we.getState() + " - " + we);
				System.exit(1);
			}
		}
	}

	private class DaemonWatcher implements Watcher {
		private AtomicInteger brokerUpdateBarrier = new AtomicInteger();
		private Set<Entry<String, String>> brokerSet = new HashSet<Entry<String, String>>();

		public void process(WatchedEvent event) {
			boolean tryUpdate = false;
			String path = event.getPath();
			daemonLogger.info("processing event: path: " + path + ", type: " + event.getType());

			if (event.getType() == Event.EventType.NodeChildrenChanged) {
				assert(path.equals(ROOT_PATH));
				tryUpdate = true;

			} else if (event.getType() == Event.EventType.NodeDeleted) {
				assert(path.endsWith(ALIVE));
				tryUpdate = true;

			} else if (event.getType() == Event.EventType.NodeDataChanged) {
				assert(path.endsWith(BROKER_FLAG));
				int brokersUpdating = brokerUpdateBarrier.decrementAndGet();
				if (brokersUpdating == 0) {
					String old = setZKNode(GLOBAL_FLAG_PATH, "0", null);
					assert(old.equals("1"));
					tryUpdate = true;
				}
			}

			try {
				long t0 = System.nanoTime();

				Set<Entry<String, String>> newBrokerSet = getLiveBrokers();
				if (tryUpdate && newBrokerSet != null)
					doUpdate(newBrokerSet);

				long t1 = System.nanoTime();
				daemonLogger.info("doUpdate took = " + (t1 - t0) + "ns");
			} catch (KeeperException e) {
				daemonLogger.error("daemonwatcher trying update - path = " + e.getPath() +
						" code = " + e.getCode() + " e: ", e);
				System.exit(1);
			} catch (InterruptedException e) {
				daemonLogger.error("daemonwatcher trying update - e = ", e);
				System.exit(1);
			}
		}

		// true if brokers changed false if no change
		// also sets watches on /padres children path and broker alive nodes
		private Set<Entry<String, String>> getLiveBrokers() throws KeeperException, InterruptedException {
			List<String> brokerList = zk.getChildren(ROOT_PATH, this);
			brokerList.remove(GLOBAL_FLAG);
			Set<Entry<String, String>> newBrokerSet = new HashSet<Entry<String, String>>();

			for (String broker : brokerList) {
				String brokerPath     = ROOT_PATH  + "/" + broker;
				String alivePath      = brokerPath + "/" + ALIVE;
				String brokerFlagPath = brokerPath + "/" + BROKER_FLAG;
				byte[] b = zk.getData(brokerPath, false, null);
				Stat as = zk.exists(alivePath, this);
				Stat bs = zk.exists(brokerFlagPath, null);

				// must retry as there is a possible (but very unlikely) race
				// where the broker is created, but its alive and flag nodes
				// are still being made
				int retry = 0;
				while ((as == null || bs == null) && retry < 5) {
					Thread.sleep(200);
					retry++;
					as = zk.exists(alivePath, this);
					bs = zk.exists(brokerFlagPath, null);
				}

				if (as != null && bs != null) {
					String uri = new String(b);
					newBrokerSet.add(new SimpleEntry<String, String>(broker, uri));
					daemonLogger.info("adding new broker = " + broker + " uri = " + uri);
				} else {
					ZKUtil.deleteRecursive(zk, brokerPath);
					daemonLogger.warn("removing broker path = " + brokerPath);
				}
			}

			Set<Entry<String, String>> result;
			if (newBrokerSet.equals(brokerSet))
				result = null;
			else
				result = newBrokerSet;

			daemonLogger.debug("current live brokers = " + brokerSet);
			daemonLogger.info("getLiveBrokers result = " + result);
			return result;
		}

		private void doUpdate(Set<Entry<String, String>> newBrokerSet) throws KeeperException, InterruptedException {
			String old = setZKNode(GLOBAL_FLAG_PATH, "1", null);
			if (old.equals("1")) {
				daemonLogger.info("tried to update, but one is currently in progress");
				return;
			}

			brokerSet = new HashSet<Entry<String, String>>(newBrokerSet);

			MainGraph newMG = new MainGraph(MG.max_hop);
			for (Entry<String, String> broker : brokerSet)
				newMG.addNode(broker.getKey(), broker.getValue());
			MG = newMG;

			// update barrier all at once before setting watchers to avoid weird races
			assert(brokerUpdateBarrier.get() == 0);
			brokerUpdateBarrier.set(brokerSet.size());

			for (Node broker : MG.graph.getEachNode()) {
				String brokerPath = ROOT_PATH + "/" + broker.getId();

				// cleanup any old neighbours
				List<String> neighbours = zk.getChildren(brokerPath, false);
				neighbours.remove(ALIVE);
				neighbours.remove(BROKER_FLAG);
				for (String neighbour : neighbours) {
					String neighbourPath = brokerPath + "/" + neighbour;
					Stat s = zk.exists(neighbourPath, false);
					zk.delete(neighbourPath, s.getVersion());
					daemonLogger.info("deleted old neighbour path = " + neighbourPath);
				}

				// add new neighbours
				Iterator<Node> neighboursIter = broker.getNeighborNodeIterator();
				while(neighboursIter.hasNext()) {
					Node neighbour = neighboursIter.next();
					String neighbourPath = brokerPath + "/" + neighbour.getId();
					String neighbourUri = neighbour.getAttribute("uri");
					initZKNode(neighbourPath, neighbourUri);
				}

				// set broker update flag
				old = setZKNode(brokerPath + "/" + BROKER_FLAG, "1", this);
				assert(old.equals("0"));
			}

			daemonLogger.info("doUpdate finished current brokers = " + brokerSet);
		}
	}

	// returns previous value
	private String setZKNode(String path, String data, Watcher watch) {
		String str = "";
		try {
			Stat s = new Stat(); 
			byte[] b = zk.getData(path, watch, s);
			str = new String(b);
			zk.setData(path, data.getBytes(), s.getVersion());
		} catch (KeeperException e) {
			daemonLogger.error("setZKNode - path = " + e.getPath() +
					" code = " + e.getCode() + " e: ", e);
			System.exit(1);
		} catch (InterruptedException e) {
			daemonLogger.error("setZKNode - e = ", e);
			System.exit(1);
		}
		return str;
	}

	private void initZKNode(String path, String data) {
		try {
			Stat s = zk.exists(path, false);
			byte [] b = data.getBytes();
			if (s == null) {
				zk.create(path, b, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				daemonLogger.info("path = " + path + " created with data = \"" + data + "\"");
			} else {
				zk.setData(path, b, s.getVersion());
				daemonLogger.warn("path = " + path + " already exists reseting data = \"" + data + "\"");
			}
		} catch (KeeperException e) {
			daemonLogger.error("initZKNode - path = " + e.getPath() +
					" code = " + e.getCode() + " e: ", e);
			System.exit(1);
		} catch (InterruptedException e) {
			daemonLogger.error("initZKNode - e = ", e);
			System.exit(1);
		}
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			daemonLogger.error("usage: daemonprocess <max hops>");
			System.exit(1);
		}

		BasicConfigurator.configure(); // for zookeeper logs
		int max_hop = Integer.parseInt(args[0]);
		new DaemonProcess(max_hop).run();
	}

	public void run() {
		try {
			synchronized (this) {
				while (true) {
					wait();
				}
			}
		} catch (InterruptedException e) {
			daemonLogger.info("daemon interrupted e: ", e);
		}
	}
}

