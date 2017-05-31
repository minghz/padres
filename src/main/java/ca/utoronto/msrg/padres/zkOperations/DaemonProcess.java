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
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

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
							" code = " + e.code() + " e: ", e);
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
		private HashMap<String, Boolean> brokerUpdateBarrier = new HashMap<String, Boolean>();
		// 0 = waiting, 1 = all brokers done cleaning, 2 = start connecting
		private int brokerState = 0;
		private Set<Entry<String, String>> brokerSet = new HashSet<Entry<String, String>>();

		public void process(WatchedEvent event) {
			boolean tryUpdate = false;
			boolean doBrokerStateChange = false;
			String path = event.getPath();
			daemonLogger.info("processing event: path: " + path + ", type: " + event.getType());

			if (event.getType() == Event.EventType.NodeChildrenChanged) {
				assert(path.equals(ROOT_PATH));
				tryUpdate = true;

			} else if (event.getType() == Event.EventType.NodeDeleted) {
				if (path.endsWith(ALIVE)) {
					tryUpdate = true;
				} else if (path.endsWith(BROKER_FLAG)) {
					// if this happens the entire update is ruined and should
					// just be restarted for simplicity we will just continue
					// to the end and catch the change with getLiveBrokers
					doBrokerStateChange = true;
				} else {
					daemonLogger.error("daemonwatcher unknown delete path = " + path);
					System.exit(1);
				}
			} else if (event.getType() == Event.EventType.NodeDataChanged) {
				assert(path.endsWith(BROKER_FLAG));
				doBrokerStateChange = true;
			} else {
				daemonLogger.warn("daemonwatcher unprocessed event = " + event + " on path = " + path);
			}

			try {
				if (doBrokerStateChange)
					tryUpdate = tryBrokerStateChange(true, path);

				Set<Entry<String, String>> newBrokerSet = getLiveBrokers();
				if (tryUpdate && newBrokerSet != null)
					doUpdate(newBrokerSet);
			} catch (KeeperException e) {
				daemonLogger.error("daemonwatcher trying update - path = " + e.getPath() +
						" code = " + e.code() + " e: ", e);
				System.exit(1);
			} catch (InterruptedException e) {
				daemonLogger.error("daemonwatcher trying update - e = ", e);
				System.exit(1);
			}
		}

		private boolean tryBrokerStateChange(boolean decrement, String path)
			throws KeeperException, InterruptedException {
			long t0 = System.nanoTime();
			boolean tryUpdate = false;

			if (decrement) {
				String newState = "-1";
				try {
					byte[] b = zk.getData(path, this, null);
					newState = new String(b);
				} catch (KeeperException e) {
					if (e.code() == KeeperException.Code.NONODE) {
						daemonLogger.warn("trybrokerstatechange broker kia = ", e);
						brokerUpdateBarrier.put(path, false);
					} else {
						throw e;
					}
				}

				if (brokerState == 1 && newState.equals("1")) {
					brokerUpdateBarrier.put(path, false);
				} else if (brokerState == 2 && newState.equals("0")) {
					brokerUpdateBarrier.put(path, false);
				} else {
					// this might not be that weird not sure
					daemonLogger.warn("trybrokerstatechange weirdness brokerState = " + brokerState
							+ " newState = " + newState + " brokerUpdateBarrier = " + brokerUpdateBarrier);
				}
			}

			if (brokerState == 1 && checkBrokerUpdateBarrier()) {
				daemonLogger.info("tryBrokerStateChange changing from 1 to 2");
				brokerState = 2;
				brokerUpdateBarrier.clear();
				// set broker flags to 2
				for (Node broker : MG.graph.getEachNode()) {
					String brokerFlagPath = ROOT_PATH + "/" + broker.getId() + "/" + BROKER_FLAG;
					brokerUpdateBarrier.put(brokerFlagPath, true);

					// watch broker update flag
					try {
						String old = setZKNode(brokerFlagPath, "2");
						assert(old.equals("1"));
						daemonLogger.debug("watching broker update flag 2 -> 0 for " + brokerFlagPath);
						byte []b = zk.getData(brokerFlagPath, this, null);
						if (new String(b).equals("0"))
							brokerUpdateBarrier.put(brokerFlagPath, false);
						// since we watch before we set it should not be
						// possible for a broker to finish connecting without
						// us getting a watch update
					} catch (KeeperException e) {
						if (e.code() == KeeperException.Code.NONODE) {
							daemonLogger.warn("trybrokerstatechange broker kia = ", e);
							brokerUpdateBarrier.put(brokerFlagPath, false);
						} else {
							throw e;
						}
					}
				}
			}
			// don't do else if as we could have quickly finished state 2 above
			if (brokerState == 2 && checkBrokerUpdateBarrier()) {
				daemonLogger.info("tryBrokerStateChange changing from 2 to 0");
				brokerState = 0;
				brokerUpdateBarrier.clear();
				// set global flag to 0
				String old = setZKNode(GLOBAL_FLAG_PATH, "0");
				assert(old.equals("1"));
				tryUpdate = true;
			}

			long t1 = System.nanoTime();
			daemonLogger.info("tryBrokerStateChange took = " + (t1 - t0) + "ns");
			return tryUpdate;
		}

		// true if brokers changed false if no change
		// also sets watches on /padres children path and broker alive nodes
		private Set<Entry<String, String>> getLiveBrokers() throws KeeperException, InterruptedException {
			long t0 = System.nanoTime();
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
				// turns out the right way to do this is check + wait for node created event on the paths
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
			long t1 = System.nanoTime();
			daemonLogger.info("getLiveBrokers took = " + (t1 - t0) + "ns");
			return result;
		}

		private void doUpdate(Set<Entry<String, String>> newBrokerSet)
			throws KeeperException, InterruptedException {
			long t0 = System.nanoTime();

			daemonLogger.debug("starting update setting global flag to 1");
			String old = setZKNode(GLOBAL_FLAG_PATH, "1");
			if (old.equals("1")) {
				daemonLogger.info("tried to update, but one is currently in progress");
				return;
			}

			brokerSet = new HashSet<Entry<String, String>>(newBrokerSet);

			assert(brokerState == 0);
			daemonLogger.debug("doUpdate brokerState changing from 0 to 1");
			brokerState = 1;

			MainGraph newMG = new MainGraph(MG.max_hop);
			for (Entry<String, String> broker : brokerSet)
				newMG.addNode(broker.getKey(), broker.getValue());
			MG = newMG;

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

				String brokerFlagPath = brokerPath + "/" + BROKER_FLAG; 
				// watch broker update flag
				try {
					brokerUpdateBarrier.put(brokerFlagPath, true);
					daemonLogger.debug("watching broker update flag 0 -> 1 for " + brokerFlagPath);
					byte[] b = zk.getData(brokerFlagPath, this, null);
					// check if broker already finished cleaning
					String brokerFlag = new String(b);
					if (brokerFlag.equals("1"))
						brokerUpdateBarrier.put(brokerFlagPath, false);
				} catch (KeeperException e) {
					if (e.code() == KeeperException.Code.NONODE)
						brokerUpdateBarrier.put(brokerFlagPath, false);
					else
						throw e;
				}
			}

			tryBrokerStateChange(false, null);

			daemonLogger.info("doUpdate finished current brokers = " + brokerSet);
			long t1 = System.nanoTime();
			daemonLogger.info("doUpdate took = " + (t1 - t0) + "ns");
		}
		private boolean checkBrokerUpdateBarrier() {
			boolean done = true;
			for (Boolean waitingOnBroker : brokerUpdateBarrier.values()) {
				if (waitingOnBroker) {
					done = false;
					break;
				}
			}
			return done;
		}
	}

	// returns previous value
	private String setZKNode(String path, String data)
		throws KeeperException, InterruptedException {
		String str = "";
		Stat s = new Stat(); 
		byte[] b = zk.getData(path, false, s);
		str = new String(b);
		zk.setData(path, data.getBytes(), s.getVersion());
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
					" code = " + e.code() + " e: ", e);
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

