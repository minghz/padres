package ca.utoronto.msrg.padres.daemon;

import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

import java.util.Iterator;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;

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


public class DaemonProcess implements Watcher, Runnable {

	protected static Logger daemonLogger = Logger.getLogger(DaemonProcess.class);

	private static final String ROOT_PATH = "/padres";
	private static final String GLOBAL_FLAG = "global_flag";
	private static final String GLOBAL_FLAG_PATH = ROOT_PATH + "/" + GLOBAL_FLAG;
	private static final String ALIVE = "/alive";
	private ZooKeeper zk;
	private ZooKeeperConnection conn;
	private MainGraph MG;

	private HashSet<String> oldBrokers = new HashSet<String>();

	public DaemonProcess(int maxHops) {
		MG = new MainGraph(maxHops);
		try {
			conn = new ZooKeeperConnection();
			zk = new ZooKeeper("localhost", 5000, this);
			zk = conn.connect("localhost");
		} catch (Exception e) {
			daemonLogger.error("message"+e.getMessage()); //Catch error message
			daemonLogger.error(e);
			System.exit(1);
		}
	}

	public void process(WatchedEvent event) {
		String path = event.getPath();

		daemonLogger.info("processing event: path: " + path + ", type: " + event.getType());

		if (event.getType() == Event.EventType.None) {
			if (event.getState() == Event.KeeperState.SyncConnected) {
				createRootPath();
				createUpdateFlag();
				try {
					handleNewBroker();
				} catch (KeeperException e) {
					daemonLogger.error("syncconnencted handleNewBroker - path = " + e.getPath() +
							" code = " + e.getCode() + " e: ", e);
					System.exit(1);
				} catch (InterruptedException e) {
					daemonLogger.error("syncconnencted handleNewBroker - e = ", e);
					System.exit(1);
				}
			} else {
				daemonLogger.error("weird event??? state = " + event.getState() + " - " + event);
				System.exit(1);
			}

		} else if (event.getType() == Event.EventType.NodeChildrenChanged) {
			assert(path.equals(ROOT_PATH));
			try {
				handleNewBroker();
			} catch (KeeperException e) {
				daemonLogger.error("childrenchanged handleNewBroker - path = " + e.getPath() +
						" code = " + e.getCode() + " e: ", e);
				System.exit(1);
			} catch (InterruptedException e) {
				daemonLogger.error("childrenchanged handleNewBroker - e = ", e);
				System.exit(1);
			}

		} else if (event.getType() == Event.EventType.NodeDeleted) {
			assert(path.endsWith(ALIVE));
			try {
				handleDeadBroker(path);
			} catch (KeeperException e) {
				daemonLogger.error("nodedeleted handleDeadBroker - path = " + e.getPath() +
						" code = " + e.getCode() + " e: ", e);
				System.exit(1);
			} catch (InterruptedException e) {
				daemonLogger.error("nodedeleted handleDeadBroker - e = ", e);
				System.exit(1);
			}
		}
	}

	private synchronized void handleDeadBroker(String path) throws KeeperException, InterruptedException {
		setGlobalFlag();

		// remove node from old graph ( /padres/|start|<brokerid>|end|/alive )
		int start = ROOT_PATH.length() + 1;
		int end = path.length() - ALIVE.length() - 1;
		String broker = path.substring(start, end);
		MG.graph.removeNode(broker);

		// create new graph
		MainGraph newMG = new MainGraph(MG.max_hop);
		for (Node n : MG.graph.getEachNode()) {
			String name = n.getId();
			String uri = n.getAttribute("uri");

			newMG.addNode(name, uri);
		}
		MG = newMG;

		updateZK();

		unsetGlobalFlag();
	}

	private synchronized void handleNewBroker() throws KeeperException, InterruptedException {
		setGlobalFlag();
		List<String> brokerList = zk.getChildren(ROOT_PATH, this);
		HashSet<String> diffBrokers = new HashSet<String>(brokerList);
		diffBrokers.removeAll(oldBrokers);

		daemonLogger.info("diff brokers = " + diffBrokers);

		while (!diffBrokers.isEmpty()) {
			for (String broker : diffBrokers) {
				if (broker.equals(GLOBAL_FLAG))
					continue;

				String brokerPath = ROOT_PATH  + "/" + broker;
				String alivePath  = brokerPath + "/" + ALIVE;

				byte[] b = zk.getData(brokerPath, false, null);
				Stat aliveStat;
				int retry = 5;
				do {
					aliveStat = zk.exists(alivePath, this);
					Thread.sleep(200);
					retry--;
				} while (aliveStat == null && retry > 0);
				if (aliveStat == null) {
					daemonLogger.error("broker not alive... skipping path = " + brokerPath);
					continue;
				}

				MG.addNode(broker, new String(b));

				writeNeighboursZK(MG.graph.getNode(broker));
			}
			oldBrokers = new HashSet<String>(brokerList);
			brokerList = zk.getChildren(ROOT_PATH, this);
			diffBrokers = new HashSet<String>(brokerList);
			diffBrokers.removeAll(oldBrokers);
		}
		unsetGlobalFlag();
	}

	private void updateZK() throws KeeperException, InterruptedException {
		for (Node broker : MG.graph.getEachNode()) {
			String brokerPath = ROOT_PATH + "/" + broker.getId();

			// clean existing neighbours
			List<String> neighbours = zk.getChildren(brokerPath, false);
			for (String neighbour : neighbours) {
				if (neighbour.equals("alive"))
					continue;

				String neighbourPath = brokerPath + "/" + neighbour;
				Stat brokerStat = zk.exists(neighbourPath, false);
				zk.delete(neighbourPath, brokerStat.getVersion());
				daemonLogger.info("deleted old neighbour path = " + neighbourPath);
			}

			writeNeighboursZK(broker);
		}
	}

	private void writeNeighboursZK(Node broker) throws KeeperException, InterruptedException {
		Iterator<Node> neighboursIter = broker.getNeighborNodeIterator();
		while(neighboursIter.hasNext()) {
			Node neighbour = neighboursIter.next();

			String neighbourPath = ROOT_PATH + "/" + broker.getId() + "/" + neighbour.getId();
			String neighbourUri = neighbour.getAttribute("uri");
			zk.create(neighbourPath, neighbourUri.getBytes(),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			daemonLogger.info("created neighbour path = " +
					neighbourPath + " data = " + neighbourUri);
		}
	}

	private void createRootPath() {
		try {
			Stat s = zk.exists(ROOT_PATH, false);
			if (s == null) {
				zk.create(ROOT_PATH, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException e) {
			daemonLogger.error("createRootPath - path = " + e.getPath() +
					" code = " + e.getCode() + " e: ", e);
			System.exit(1);
		} catch (InterruptedException e) {
			daemonLogger.error("createRootPath - e = ", e);
			System.exit(1);
		}
	}

	// TODO: ensure only 1 daemon running (and allow for daemon to be
	// restarted) with PID node of currently running daemon
	private synchronized void createUpdateFlag() {
		try {
			byte[] b = "0".getBytes();
			Stat flagStat = zk.exists(GLOBAL_FLAG_PATH, false);
			if (flagStat != null) {
				daemonLogger.warn("flag already exists make sure no other daemons are running...");
				zk.setData(GLOBAL_FLAG_PATH, b, flagStat.getVersion());
			} else {
				zk.create(GLOBAL_FLAG_PATH, b, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException e) {
			daemonLogger.error("createUpdateFlag - path = " + e.getPath() +
					" code = " + e.getCode() + " e: ", e);
			System.exit(1);
		} catch (InterruptedException e) {
			daemonLogger.error("createUpdateFlag - e = ", e);
			System.exit(1);
		}
	}

	private synchronized void setGlobalFlag() {
		try {
			Stat flagStat = new Stat(); 
			byte[] flag = zk.getData(GLOBAL_FLAG_PATH, false, flagStat);
			assert(new String(flag).equals("0"));
			// should not be possible to already be in an update...
			zk.setData(GLOBAL_FLAG_PATH, "1".getBytes(), flagStat.getVersion());
		} catch (KeeperException e) {
			daemonLogger.error("setGlobalFlag - path = " + e.getPath() +
					" code = " + e.getCode() + " e: ", e);
			System.exit(1);
		} catch (InterruptedException e) {
			daemonLogger.error("setGlobalFlag - e = ", e);
			System.exit(1);
		}
	}

	private synchronized void unsetGlobalFlag() {
		try {
			Stat flagStat = new Stat(); 
			byte[] flag = zk.getData(GLOBAL_FLAG_PATH, false, flagStat);
			assert(new String(flag).equals("0"));
			// should not be possible to already be out of an update...
			zk.setData(GLOBAL_FLAG_PATH, "0".getBytes(), flagStat.getVersion());
		} catch (KeeperException e) {
			daemonLogger.error("unsetGlobalFlag - path = " + e.getPath() +
					" code = " + e.getCode() + " e: ", e);
			System.exit(1);
		} catch (InterruptedException e) {
			daemonLogger.error("unsetGlobalFlag - e = ", e);
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

