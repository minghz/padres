// =============================================================================
// This file is part of The PADRES Project.
//
// For more information, see http://www.msrg.utoronto.ca
//
// Copyright (c) 2003 Middleware Systems Research Group, University of Toronto
// =============================================================================
// $Id$
// =============================================================================

/*
 * Created on 16-Jul-2003
 */
package ca.utoronto.msrg.padres.broker.brokercore;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.EnumSet;

import javax.swing.Timer;

import ca.utoronto.msrg.padres.common.comm.*;
import org.apache.log4j.Logger;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZKUtil;

import ca.utoronto.msrg.padres.broker.brokercore.BrokerConfig.CycleType;
import ca.utoronto.msrg.padres.broker.controller.Controller;
import ca.utoronto.msrg.padres.broker.controller.LinkInfo;
import ca.utoronto.msrg.padres.broker.controller.OverlayManager;
import ca.utoronto.msrg.padres.broker.controller.OverlayRoutingTable;
import ca.utoronto.msrg.padres.broker.management.console.ConsoleInterface;
import ca.utoronto.msrg.padres.broker.management.web.ManagementServer;
import ca.utoronto.msrg.padres.broker.monitor.SystemMonitor;
import ca.utoronto.msrg.padres.broker.router.Router;
import ca.utoronto.msrg.padres.broker.router.RouterFactory;
import ca.utoronto.msrg.padres.broker.router.matching.MatcherException;
import ca.utoronto.msrg.padres.broker.webmonitor.monitor.WebUIMonitorToBeRemoved;
import ca.utoronto.msrg.padres.common.message.AdvertisementMessage;
import ca.utoronto.msrg.padres.common.message.Unadvertisement;
import ca.utoronto.msrg.padres.common.message.UnadvertisementMessage;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.MessageDestination.DestinationType;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;
import ca.utoronto.msrg.padres.common.message.Unsubscription;
import ca.utoronto.msrg.padres.common.message.UnsubscriptionMessage;
import ca.utoronto.msrg.padres.common.util.CommandLine;
import ca.utoronto.msrg.padres.common.util.LogException;
import ca.utoronto.msrg.padres.common.util.LogSetup;
import ca.utoronto.msrg.padres.common.util.timer.TimerThread;

/**
 * The core of the broker. The broker is instantiated through this class. BrokerCore provides unique
 * message ID generation, component location, message routing.
 * 
 * @author eli
 * 
 */
public class BrokerCore {

	protected BrokerConfig brokerConfig;

	protected Controller controller;

	protected SystemMonitor systemMonitor;

	protected QueueManager queueManager;

	protected InputQueueHandler inputQueue;

	protected Router router;

	protected WebUIMonitorToBeRemoved webuiMonitorToBeRemoved;

	protected TimerThread timerThread;

	protected HeartbeatPublisher heartbeatPublisher;

	protected HeartbeatSubscriber heartbeatSubscriber;

	protected MessageDestination brokerDestination;

	protected int currentMessageID;

	protected CommSystem commSystem;

	protected boolean debug = BrokerConfig.DEBUG_MODE_DEFAULT;

	// Indicates whether this broker is running or not
	protected boolean running = false;

	protected boolean isCycle = false;

	protected boolean isDynamicCycle = false;

	// for dynamic cycle, check message rate. the time_window_interval can also be defined in the
	// broker property file
	protected int time_window_interval = 5000;

	private boolean isShutdown = false;

	protected static Logger brokerCoreLogger;

	protected static Logger exceptionLogger;

	/**
	 * Constructor for one argument. To take advantage of command line arguments, use the
	 * 'BrokerCore(String[] args)' constructor
	 * 
	 * @param arg
	 * @throws IOException
	 */
	public BrokerCore(String arg) throws BrokerCoreException {
		this(arg.split("\\s+"));
	}

	public BrokerCore(String[] args, boolean def) throws BrokerCoreException {
		if (args == null) {
			throw new BrokerCoreException("Null arguments");
		}
		CommandLine cmdLine = new CommandLine(BrokerConfig.getCommandLineKeys());
		try {
			cmdLine.processCommandLine(args);
		} catch (Exception e) {
			throw new BrokerCoreException("Error processing command line", e);
		}
		// make sure the logger is initialized before everything else
		initLog(cmdLine.getOptionValue(BrokerConfig.CMD_ARG_FLAG_LOG_LOCATION));
		brokerCoreLogger.debug("BrokerCore is starting.");
		// load properties from given/default properties file get the broker configuration
		String configFile = cmdLine.getOptionValue(BrokerConfig.CMD_ARG_FLAG_CONFIG_PROPS);
		try {
			if (configFile == null)
				brokerConfig = new BrokerConfig();
			else
				brokerConfig = new BrokerConfig(configFile, def);
		} catch (BrokerCoreException e) {
			brokerCoreLogger.fatal(e.getMessage(), e);
			exceptionLogger.fatal(e.getMessage(), e);
			throw e;
		}
		// overwrite the configurations from the config file with the configurations from the
		// command line
		brokerConfig.overwriteWithCmdLineArgs(cmdLine);
		// check broker configuration
		try {
			brokerConfig.checkConfig();
		} catch (BrokerCoreException e) {
			brokerCoreLogger.fatal("Missing uri key or uri value in the property file.");
			exceptionLogger.fatal("Here is an exception : ", e);
			throw e;
		}
		// initialize the message sequence counter
		currentMessageID = 0;
	}
	
	/**
	 * Constructor
	 * 
	 * @param args
	 */
	public BrokerCore(String[] args) throws BrokerCoreException {
		this(args, true);
	}

	public BrokerCore(BrokerConfig brokerConfig) throws BrokerCoreException {
		// make sure the logger is initialized before everything else
		initLog(brokerConfig.getLogDir());
		brokerCoreLogger.debug("BrokerCore is starting.");
		this.brokerConfig = brokerConfig;
		try {
			this.brokerConfig.checkConfig();
		} catch (BrokerCoreException e) {
			brokerCoreLogger.fatal("Missing uri key or uri value in the property file.");
			exceptionLogger.fatal("Here is an exception : ", e);
			throw e;
		}
		currentMessageID = 0;
	}

	protected void initLog(String logPath) throws BrokerCoreException {
		if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
			try {
				new LogSetup(logPath);
			} catch (LogException e) {
				throw new BrokerCoreException("Initialization of Logger failed: ", e);
			}
		}
		brokerCoreLogger = Logger.getLogger(BrokerCore.class);
		exceptionLogger = Logger.getLogger("Exception");
	}

	/**
	 * Initialize the broker. It has to be called externally; the constructor does not use this
	 * method. Components are started up in a particular order, and initialize() doesn't return
	 * until the broker is fully started.
	 * 
	 * @throws BrokerCoreException
	 */
	public void initialize() throws BrokerCoreException {
		// Initialize some parameters
		isCycle = brokerConfig.isCycle();
		isDynamicCycle = brokerConfig.getCycleOption() == CycleType.DYNAMIC;
		// Initialize components
		initCommSystem();
		brokerDestination = new MessageDestination(getBrokerURI(), DestinationType.BROKER);
		initQueueManager();
		initRouter();
		initInputQueue();
		// System monitor must be started before sending/receiving any messages
		initSystemMonitor();
		initController();
		startMessageRateTimer();
		initTimerThread();
		initHeartBeatPublisher();
		initHeartBeatSubscriber();
		initWebInterface();
		initManagementInterface();
		initConsoleInterface();
		initZKConnection();
		running = true;
		brokerCoreLogger.info("BrokerCore is started.");
	}

	protected class ZKConnect implements Watcher {
		private ZooKeeper zk;

		public ZKConnect(String zkHost) throws BrokerCoreException {
			try {
				this.zk = new ZooKeeper(zkHost, 5000, this);
			} catch (IOException e) {
				throw new BrokerCoreException("Failed to connect to ZooKeeper: " + zkHost + " with: ", e);
			}
		}

		// TODO: can maybe retry zookeeper connection, but if the broker's zk
		// path isn't setup right correctly the broker should die
		public void process(WatchedEvent we) {
			if (we.getState() == Event.KeeperState.SyncConnected) {
				try {
					BrokerWatcher bw = new BrokerWatcher(zk, getBrokerNodeID());
					// check/watch broker flag for update
					bw.tryGlobalStateChange();

				} catch (KeeperException e) {
					brokerCoreLogger.error("zkconnect creating brokerwatcher - path = " + e.getPath() +
							" code = " + e.code() + " e: ", e);
					System.exit(1);
				} catch (InterruptedException e) {
					brokerCoreLogger.error("zkconnect creating brokerwatcher - e = ", e);
					System.exit(1);
				}
			} else {
				brokerCoreLogger.error("weird event??? state = " + we.getState() + " - " + we);
				System.exit(1);
			}
		}
	}

	protected class BrokerWatcher implements Watcher {
		// this is duplicated in DaemonProcess should consolidate...
		private static final String ROOT_PATH = "/padres";
		private static final String GLOBAL_FLAG = "global_flag";
		private static final String GLOBAL_FLAG_PATH = ROOT_PATH + "/" + GLOBAL_FLAG;
		private static final String ALIVE = "alive";
		private static final String BROKER_FLAG = "broker_flag";
		private String BROKER_PATH;
		private String ALIVE_PATH;
		private String BROKER_FLAG_PATH;
		private ZooKeeper zk;

		private String[] EXCLUDE_CLASS_NAMES = new String[] { "BROKER_CONTROL", "BROKER_INFO",
			"BROKER_MONITOR", "GLOBAL_FD", "HEARTBEAT_MANAGER", "NETWORK_DISCOVERY",
			"TRACEROUTE_MESSAGE" };
		private HashSet<String> EXCLUDE_CLASSES = new HashSet<String>(Arrays.asList(EXCLUDE_CLASS_NAMES));


		private Map<String, AdvertisementMessage> savedAdvs;
		private Map<String, SubscriptionMessage> savedSubs;

		// should be enum... 0 = not updating, 1 = updating
		private int globalState = 0;

		public BrokerWatcher(ZooKeeper zk, String brokerId) throws KeeperException, InterruptedException {
			this.zk = zk;
			BROKER_PATH      = ROOT_PATH   + "/" + brokerId;
			ALIVE_PATH       = BROKER_PATH + "/" + ALIVE;
			BROKER_FLAG_PATH = BROKER_PATH + "/" + BROKER_FLAG;
			savedAdvs = new ConcurrentHashMap<String, AdvertisementMessage>();
			savedSubs = new ConcurrentHashMap<String, SubscriptionMessage>();

			Stat bs = zk.exists(BROKER_PATH, false);
			if (bs != null) {
				Stat as = zk.exists(ALIVE_PATH, false);
				if (as == null) {
					// cleanup old broker data
					ZKUtil.deleteRecursive(zk, BROKER_PATH);
				} else {
					// uri binding should fail and crash before this...
					brokerCoreLogger.error("broker " + BROKER_PATH + " is still alive...");
					System.exit(1);
				}
			}

			zk.create(BROKER_PATH, getBrokerURI().getBytes(),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create(ALIVE_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			zk.create(BROKER_FLAG_PATH, "0".getBytes(),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		}

		public void process(WatchedEvent we) {
			String path = we.getPath();
			brokerCoreLogger.info("b = " + getBrokerID() + " processing event: path: " +
					path + ", type: " + we.getType());
			if (we.getType() == Event.EventType.NodeDataChanged) {
				try {
					if (path.equals(GLOBAL_FLAG_PATH))
						tryGlobalStateChange();
					else if (path.equals(BROKER_FLAG_PATH))
						doConnect();
					else
						brokerCoreLogger.error("brokerwatcher unknown path = " + path);
				} catch (KeeperException e) {
					brokerCoreLogger.error("brokerwatcher trying update - path = " + path +
							" code = " + e.code() + " e: ", e);
					System.exit(1);
				} catch (InterruptedException e) {
					brokerCoreLogger.error("brokerwatcher trying update - e = ", e);
					System.exit(1);
				}
			} else {
				brokerCoreLogger.error("BrokerWatcher unhandled event: " + we);
				// TODO: ideally defend against zookeeper going down somehow?
			}
		}

		private void doConnect() throws KeeperException, InterruptedException {
			long t0 = System.nanoTime();
			byte[] b = zk.getData(BROKER_FLAG_PATH, false, null);
			if (!(new String(b).equals("2")))
				return;

			// create new connections
			HashSet<String> newNeighbourURIs = new HashSet<String>();
			List<String> newNeighbours = zk.getChildren(BROKER_PATH, false);
			newNeighbours.remove(ALIVE);
			newNeighbours.remove(BROKER_FLAG);
			for (String neighbour : newNeighbours) {
				String neighbourPath = BROKER_PATH + "/" + neighbour;
				String neighbourURI;
				b = zk.getData(neighbourPath, false, null);
				neighbourURI = new String(b);
				newNeighbourURIs.add(neighbourURI);

				Publication p = MessageFactory.createEmptyPublication();
				p.addPair("class", "BROKER_CONTROL");
				p.addPair("brokerID", getBrokerID());
				p.addPair("command", "OVERLAY-UPDATE");
				p.addPair("broker", neighbourURI);
				PublicationMessage pm = new PublicationMessage(p, "initial_connect");
				brokerCoreLogger.debug("Broker " + getBrokerID()
						+ " is sending initial connection to broker " + neighbourURI);
				routeMessage(pm, MessageDestination.INPUTQUEUE);
			}
			
			HashSet<String> curNeighbourURIs = new HashSet<String>();
			Map<MessageDestination, OutputQueue> neighbours = getOverlayManager().getORT().getBrokerQueues();
			synchronized (neighbours) {
				for (MessageDestination neighbour : neighbours.keySet())
					curNeighbourURIs.add(neighbour.getDestinationID());
			}
			int retry = 0;
			while (!curNeighbourURIs.containsAll(newNeighbourURIs) && retry < 5) {
				Thread.sleep(200);
				retry++;
			}

			// finished new connections -> update broker flag
			Stat s = new Stat(); 
			b = zk.getData(BROKER_FLAG_PATH, null, s);
			assert(new String(b).equals("2"));
			brokerCoreLogger.debug("b = " + getBrokerID() + " setting update flag 2 -> 0");
			zk.setData(BROKER_FLAG_PATH, "0".getBytes(), s.getVersion());

			long t1 = System.nanoTime();
			brokerCoreLogger.info("b = " + getBrokerID() + " doConnect took = " + (t1 - t0) + "ns");

			// check/watch global flag for when all brokers are done
			tryGlobalStateChange();
		}

		private void tryGlobalStateChange() throws KeeperException, InterruptedException {
			byte[] b = zk.getData(GLOBAL_FLAG_PATH, this, null);
			String newState = new String(b);
			if (globalState == 0 && newState.equals("1")) {
				brokerCoreLogger.info("b = " + getBrokerID() + " tryGlobalStateChange changing from 0 to 1");
				globalState = 1;
				cleanState();
			} else if (globalState == 1 && newState.equals("0")) {
				brokerCoreLogger.info("tryGlobalStateChange changing from 1 to 0");
				globalState = 0;
				resumeWork();
			}
			/* no reason to log this as it happens expectedly
			else {
				brokerCoreLogger.warn("tryGlobalStateChange bad = " +
						globalState + " newstate = " + newState);
			}
			*/
		}

		private void resumeWork() throws KeeperException, InterruptedException {
			reSendAdvSub();

			// check/watch broker flag for update
			tryGlobalStateChange();
		}

		// TODO: clean up old connections + missing message types + possibly other unknown things
		private void cleanState() throws KeeperException, InterruptedException {
			long t0 = System.nanoTime();
			brokerCoreLogger.debug("b = " + getBrokerID() + " cleaning state");
			// remove neighbour queues
			//OverlayRoutingTable ort = getOverlayManager().getORT();
			//Map<MessageDestination, OutputQueue> neighbours = ort.getBrokerQueues();
			//for (MessageDestination neighbour : neighbours.keySet()) {
			//    brokerCoreLogger.debug("b = " + getBrokerID() + " removing neighbour = " + neighbour);
			//    ort.removeBroker(neighbour);
			//    removeQueue(neighbour);
			//}
			//brokerCoreLogger.debug("b = " + getBrokerID() + " done removing neighbour");

			savedAdvs.clear();
			savedSubs.clear();
			//brokerCoreLogger.debug("b = " + getBrokerID() + " workingAdvs = " + getAdvertisements());
			saveAndCleanAdvs();
			//brokerCoreLogger.debug("b = " + getBrokerID() + " savedAdvs = " + savedAdvs);
			//brokerCoreLogger.debug("b = " + getBrokerID() + " workingSubs = " + getSubscriptions());
			saveAndCleanSubs();
			//brokerCoreLogger.debug("b = " + getBrokerID() + " savedSubs = " + savedSubs);
			//saveAndCleanRoutedSubs();
			//brokerCoreLogger.debug("b = " + getBrokerID() + " routedSubs = " + router.getRoutedSubs());

			// finished cleaning -> update broker flag
			Stat s = new Stat(); 
			byte[] b = zk.getData(BROKER_FLAG_PATH, null, s);
			assert(new String(b).equals("0"));
			brokerCoreLogger.debug("b = " + getBrokerID() + " setting update flag 0 -> 1");
			zk.setData(BROKER_FLAG_PATH, "1".getBytes(), s.getVersion());

			long t1 = System.nanoTime();
			brokerCoreLogger.info("b = " + getBrokerID() + " cleanState took = " + (t1 - t0) + "ns");

			// check/watch broker flag for when all brokers are done cleaning (to start connecting)
			b = zk.getData(BROKER_FLAG_PATH, this, null);
			if (new String(b).equals("2"))
				doConnect();
		}

		// TODO: switch to .forEachValue for concurrency?
		private void saveAndCleanAdvs() {
			Iterator<Map.Entry<String, AdvertisementMessage>> iter = getAdvertisements().entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String, AdvertisementMessage> e = iter.next();
				String msgId = e.getKey();
				AdvertisementMessage msg = e.getValue();
				EnumSet<DestinationType> dstType = msg.getLastHopID().getDestinationType();

				// if last hop is a system interal do not save or remove it
				// if last hop is a client save it to resend
				// if it is anything else (routing + connection) remove
				// note: just checking for destination type internal works for subs, but not advs
				//if (dstType.contains(DestinationType.INTERNAL))

				//brokerCoreLogger.debug("b = " + getBrokerID() + " WTFFADV msg = " + msg + " msgID = "
				//        + msgId + " dstType = " + dstType + " avdid = "
				//        + msg.getAdvertisement().getAdvID());

				if (dstType.contains(DestinationType.CLIENT)) {
					brokerCoreLogger.debug("b = " + getBrokerID() + " WTFFADV msg = " + msg + " msgID = "
							+ msgId + " dstType = " + dstType + " avdid = "
							+ msg.getAdvertisement().getAdvID());
					savedAdvs.put(msg.getMessageID(), msg);
					Unadvertisement unadv = new Unadvertisement(msg.getMessageID());
					UnadvertisementMessage unadvmsg = new UnadvertisementMessage(unadv, getNewMessageID());
					brokerCoreLogger.debug("b = " + getBrokerID() + " sending unadv = " + unadvmsg +
							" advid = " + unadvmsg.getUnadvertisement().getAdvID());
					routeMessage(unadvmsg, MessageDestination.INPUTQUEUE);

				} else if (!msgId.startsWith(getBrokerID()) ||
						!EXCLUDE_CLASSES.contains(msg.getAdvertisement().getClassVal())) {

					Unadvertisement unadv = new Unadvertisement(msg.getMessageID());
					UnadvertisementMessage unadvmsg = new UnadvertisementMessage(unadv, getNewMessageID());
					brokerCoreLogger.debug("b = " + getBrokerID() + " sending unadv = " + unadvmsg +
							" advid = " + unadvmsg.getUnadvertisement().getAdvID());
					routeMessage(unadvmsg, MessageDestination.INPUTQUEUE);
				}
				//else if (!EXCLUDE_CLASSES.contains(msg.getAdvertisement().getClassVal())) {
				//    iter.remove();
				//    brokerCoreLogger.debug("b = " + getBrokerID() + " removing adv = " + msg +
				//            " advid = " + msg.getAdvertisement().getAdvID());
				//}
			}
		}

		private void saveAndCleanSubs() {
			Iterator<Map.Entry<String, SubscriptionMessage>> iter = getSubscriptions().entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String, SubscriptionMessage> e = iter.next();
				String msgId = e.getKey();
				SubscriptionMessage msg = e.getValue();
				EnumSet<DestinationType> dstType = msg.getLastHopID().getDestinationType();

				// if last hop is a system interal do not save or remove it
				// if last hop is a client save it to resend
				// if it is anything else (routing + connection) remove
				// note: just checking for destination type internal works for subs, but not advs
				//if (dstType.contains(DestinationType.INTERNAL))

				//brokerCoreLogger.debug("b = " + getBrokerID() + " WTFFSUB msg = " + msg + " msgID = "
				//        + msgId + " dstType = " + dstType + " subid = "
				//        + msg.getSubscription().getSubscriptionID());

				if (dstType.contains(DestinationType.CLIENT)) {
					brokerCoreLogger.debug("b = " + getBrokerID() + " WTFFSUB msg = " + msg + " msgID = "
							+ msgId + " dstType = " + dstType + " subid = "
							+ msg.getSubscription().getSubscriptionID());
					savedSubs.put(msg.getMessageID(), msg);

					Unsubscription unsub = new Unsubscription(msg.getMessageID());
					UnsubscriptionMessage unsubmsg = new UnsubscriptionMessage(unsub, getNewMessageID());
					brokerCoreLogger.debug("b = " + getBrokerID() + " sending unsub = " + unsubmsg +
							" subid = " + unsubmsg.getUnsubscription().getSubID());
					routeMessage(unsubmsg, MessageDestination.INPUTQUEUE);

				} else if (!msgId.startsWith(getBrokerID()) ||
						!EXCLUDE_CLASSES.contains(msg.getSubscription().getClassVal())) {

					Unsubscription unsub = new Unsubscription(msg.getMessageID());
					UnsubscriptionMessage unsubmsg = new UnsubscriptionMessage(unsub, getNewMessageID());
					brokerCoreLogger.debug("b = " + getBrokerID() + " sending unsub = " + unsubmsg +
							" subid = " + unsubmsg.getUnsubscription().getSubID());
					routeMessage(unsubmsg, MessageDestination.INPUTQUEUE);
				}
				//else if (!EXCLUDE_CLASSES.contains(msg.getSubscription().getClassVal())) {
				//    iter.remove();
				//    brokerCoreLogger.debug("b = " + getBrokerID() + " removing sub = " + msg +
				//            " subid = " + msg.getSubscription().getSubscriptionID());
				//}
			}
		}

		private void saveAndCleanRoutedSubs() {
			Iterator<Map.Entry<String, Set<MessageDestination>>> routedSubIter =
				router.getRoutedSubs().entrySet().iterator();
			while (routedSubIter.hasNext()) {
				Map.Entry<String, Set<MessageDestination>> routedSubEntry = routedSubIter.next();
				if (!routedSubEntry.getKey().startsWith(getBrokerID())) {
					routedSubIter.remove();
				} else {
					Iterator<MessageDestination> dstIter = routedSubEntry.getValue().iterator();
					while (dstIter.hasNext()) {
						MessageDestination dst = dstIter.next();
						if (!dst.getDestinationType().contains(DestinationType.INTERNAL) && !dst.getDestinationID().equals("none")) {
							dstIter.remove();
						}
					}
				}
			}
		}

		private void reSendAdvSub() {
			for (AdvertisementMessage advmsg : savedAdvs.values()) {
				brokerCoreLogger.debug("b = " + getBrokerID() + " resending adv = " + advmsg);
				routeMessage(advmsg.duplicate(), MessageDestination.INPUTQUEUE);
			}

			for (SubscriptionMessage submsg : savedSubs.values()) {
				brokerCoreLogger.debug("b = " + getBrokerID() + " resending sub = " + submsg);
				routeMessage(submsg.duplicate(), MessageDestination.INPUTQUEUE);
			}
		}
	}

	protected void initZKConnection() throws BrokerCoreException {
		String zkHost = brokerConfig.getZKHost();
		if (zkHost == null) {
			return;
		}

		ZKConnect zkc = new ZKConnect(zkHost);
		//TODO: save zkc?
	}

	/**
	 * Initialize the communication layer in the connection listening mode.
	 * 
	 * @throws BrokerCoreException
	 */
	protected void initCommSystem() throws BrokerCoreException {
		// initialize the communication interface
		try {
			commSystem = createCommSystem();
			commSystem.createListener(brokerConfig.brokerURI);
			brokerCoreLogger.info("Communication System created and a listening server is initiated");
		} catch (CommunicationException e) {
			brokerCoreLogger.error("Communication layer failed to instantiate: " + e);
			exceptionLogger.error("Communication layer failed to instantiate: " + e);
			throw new BrokerCoreException("Communication layer failed to instantiate: " + e + "\t" + brokerConfig.brokerURI);
		}
	}

	/**
	 * Initialize the message queue manager which acts as a multiplexer between the communication
	 * layer and all the queues for different internal components as well as external connections.
	 * Initialize the queue manager only after initialzing the communication layer.
	 * 
	 * @throws BrokerCoreException
	 */
	protected void initQueueManager() throws BrokerCoreException {
		queueManager = createQueueManager();
		brokerCoreLogger.info("Queue Manager is created");
	}

	protected QueueManager createQueueManager() throws BrokerCoreException {
		return new QueueManager(this);
	}

	/**
	 * Initialize the router.
	 * 
	 * @throws BrokerCoreException
	 */
	protected void initRouter() throws BrokerCoreException {
		try {
			router = RouterFactory.createRouter(brokerConfig.matcherName, this);
			router.initialize();
			brokerCoreLogger.info("Router/Matching Engine is initialized");
		} catch (MatcherException e) {
			brokerCoreLogger.error("Router failed to instantiate: " + e);
			exceptionLogger.error("Router failed to instantiate: " + e);
			throw new BrokerCoreException("Router failed to instantiate: " + e);
		}
	}

	/**
	 * Initialize the input queue that is the first place a message enters from communication layer.
	 * It exploits the router to redirect traffic to different other queues.
	 * 
	 * @throws BrokerCoreException
	 */
	protected void initInputQueue() throws BrokerCoreException {
		inputQueue = createInputQueueHandler();
		inputQueue.start();
		registerQueue(inputQueue);
		brokerCoreLogger.debug("InputQueueHandler is starting.");
		try {
			inputQueue.waitUntilStarted();
		} catch (InterruptedException e) {
			brokerCoreLogger.error("InputQueueHandler failed to start: " + e);
			exceptionLogger.error("InputQueueHandler failed to start: " + e);
			throw new BrokerCoreException("InputQueueHandler failed to start", e);
		}
		brokerCoreLogger.info("InputQueueHandler is started.");
	}
	
	protected InputQueueHandler createInputQueueHandler() {
		return new InputQueueHandler(this);
	}
	
	protected Controller createController() {
		return new Controller(this);
	}
	
	protected CommSystem createCommSystem() throws CommunicationException {
		return new CommSystem();
	}


	/**
	 * Initialize the system monitor which collects broker system information. QueueManager and
	 * InputQueue must have been initialized before using this method.
	 * 
	 * @throws BrokerCoreException
	 */
	protected void initSystemMonitor() throws BrokerCoreException {
		systemMonitor = createSystemMonitor();
		brokerCoreLogger.info("System Monitor is created");
		// register the system monitor with queue manager and input queue, so that they can feed
		// data into the monitor
		if (queueManager == null)
			throw new BrokerCoreException(
					"QueueManager must have been initialized before SystemMonitor");
		queueManager.registerSystemMonitor(systemMonitor);
		if (inputQueue == null)
			throw new BrokerCoreException(
					"InputQueue must have been initialized before SystemMonitor");
		inputQueue.registerSystemMonitor(systemMonitor);
		systemMonitor.start();
		brokerCoreLogger.debug("System monitor is starting.");
		try {
			systemMonitor.waitUntilStarted();
		} catch (InterruptedException e) {
			brokerCoreLogger.error("System monitor failed to start: " + e);
			exceptionLogger.error("System monitor failed to start: " + e);
			throw new BrokerCoreException("System monitor failed to start", e);
		}
		brokerCoreLogger.info("System monitor is started.");
	}

	protected SystemMonitor createSystemMonitor() {
		return new SystemMonitor(this);
	}

	protected void initController() throws BrokerCoreException {
		controller = createController();
		controller.start();
		brokerCoreLogger.debug("Controller is starting.");
		try {
			controller.waitUntilStarted();
		} catch (InterruptedException e) {
			brokerCoreLogger.error("Controller failed to start: " + e);
			exceptionLogger.error("Controller failed to start: " + e);
			throw new BrokerCoreException("Controller failed to start", e);
		}
		brokerCoreLogger.info("Controller is started.");
	}

	protected void startMessageRateTimer() {
		ActionListener checkMsgRateTaskPerformer = new ActionListener() {

			public void actionPerformed(ActionEvent evt) {
				OverlayRoutingTable ort = getOverlayManager().getORT();
				Map<MessageDestination, LinkInfo> statisticTable = ort.getStatisticTable();
				Map<MessageDestination, OutputQueue> neighbors = ort.getBrokerQueues();
				synchronized (neighbors) {
					for (MessageDestination temp : neighbors.keySet()) {
						if (statisticTable.containsKey(temp)) {
							LinkInfo tempLink = statisticTable.get(temp);
							if (inputQueue.containsDest(temp)) {
								Integer tempI = inputQueue.getNum(temp);
								tempLink.setMsgRate(tempI.intValue());
								inputQueue.setNum(temp, new Integer(0));
							}
						}
					}
				}
			}
		};
		Timer msgRateTimer = new Timer(time_window_interval, checkMsgRateTaskPerformer);
		msgRateTimer.start();
	}

	protected void initTimerThread() throws BrokerCoreException {
		// start the timer thread (for timing heartbeats)
		timerThread = new TimerThread();
		timerThread.start();
		brokerCoreLogger.debug("TimerThread is starting.");
		try {
			timerThread.waitUntilStarted();
		} catch (InterruptedException e) {
			brokerCoreLogger.error("TimerThread failed to start: " + e);
			exceptionLogger.error("TimerThread failed to start: " + e);
			throw new BrokerCoreException("TimerThread failed to start", e);
		}
		brokerCoreLogger.info("TimerThread is started.");
	}

	protected void initHeartBeatPublisher() throws BrokerCoreException {
		// start the heartbeat publisher thread
		heartbeatPublisher = new HeartbeatPublisher(this);
		heartbeatPublisher.setPublishHeartbeats(brokerConfig.isHeartBeat());
		heartbeatPublisher.start();
		brokerCoreLogger.debug("HeartbeatPublisher is starting.");
		try {
			heartbeatPublisher.waitUntilStarted();
		} catch (InterruptedException e) {
			brokerCoreLogger.error("HeartbeatPublisher failed to start: " + e);
			exceptionLogger.error("HeartbeatPublisher failed to start: " + e);
			throw new BrokerCoreException("HeartbeatPublisher failed to start", e);
		}
		brokerCoreLogger.info("HeartbeatPublisher is started.");
	}

	protected void initHeartBeatSubscriber() throws BrokerCoreException {
		// start the heartbeat subscriber thread
		heartbeatSubscriber = createHeartbeatSubscriber();
		heartbeatSubscriber.start();
		brokerCoreLogger.debug("HeartbeatSubscriber is starting.");
		try {
			heartbeatSubscriber.waitUntilStarted();
		} catch (InterruptedException e) {
			brokerCoreLogger.error("HeartbeatSubscriber failed to start: " + e);
			exceptionLogger.error("HeartbeatSubscriber failed to start: " + e);
			throw new BrokerCoreException("HeartbeatSubscriber failed to start", e);
		}
		brokerCoreLogger.info("HeartbeatSubscriber is started.");
	}

	protected HeartbeatSubscriber createHeartbeatSubscriber() {
		return new HeartbeatSubscriber(this);
	}

	protected void initWebInterface() {
		if (brokerConfig.isWebInterface()) {
			ManagementServer managementServer = new ManagementServer(this);
			managementServer.start();
		}
	}

	protected void initManagementInterface() {
		if (brokerConfig.isManagementInterface()) {			
			// start the management interface web server
			webuiMonitorToBeRemoved = new WebUIMonitorToBeRemoved(this);
			webuiMonitorToBeRemoved.initialize();
			brokerCoreLogger.info("ManagementInterface is started.");
		}
	}

	protected void initConsoleInterface() {
		if (brokerConfig.isCliInterface()) {
			ConsoleInterface consoleInterface = new ConsoleInterface(this);
			consoleInterface.start();
		}
	}

	/**
	 * @return The configuration of the broker
	 */
	public BrokerConfig getBrokerConfig() {
		return brokerConfig;
	}

	public WebUIMonitorToBeRemoved getWebuiMonitorToBeRemoved() {
		return webuiMonitorToBeRemoved;
	}

	/**
	 * @return The
	 */
	public String getDBPropertiesFile() {
		return brokerConfig.getDbPropertyFileName();
	}

	public String getMIPropertiesFile() {
		return brokerConfig.getManagementPropertyFileName();
	}

	/**
	 * @return The ID of the broker
	 */
	public String getBrokerID() {
		return getBrokerURI();
	}

	/**
	 * @return The MessageDestination for the broker.
	 */
	public MessageDestination getBrokerDestination() {
		return brokerDestination;
	}

	/**
	 * @return
	 */
	public String getBrokerURI() {
		try {
//			return commSystem.getServerURI();
			return ConnectionHelper.getAddress(brokerConfig.brokerURI).getNodeURI();
		} catch (CommunicationException e) {
			e.printStackTrace();
			System.exit(1);
		}
		return null;
	}

	public String getBrokerNodeID() {
		try {
			return ConnectionHelper.getAddress(brokerConfig.brokerURI).getNodeID();
		} catch (CommunicationException e) {
			e.printStackTrace();
			System.exit(1);
		}
		return null;
	}

	/**
	 * Get a new (globally unique) message ID
	 * 
	 * @return The new message ID
	 */
	public synchronized String getNewMessageID() {
		return getBrokerID() + "-M" + currentMessageID++;
	}

	public MessageListenerInterface getMessageListener() {
		return queueManager;
	}

	/**
	 * Route a Message to a given destination. Errors are handled by the queueManager.
	 * 
	 * @param msg
	 *            The message to send
	 * @param destination
	 *            The destination for the message
	 */
	public void routeMessage(Message msg, MessageDestination destination) {
		queueManager.enQueue(msg, destination);
	}

	/**
	 * Route a Message to its nextHopID. Errors are handled by the queueManager.
	 * 
	 * @param msg
	 *            The message to send
	 */
	public void routeMessage(Message msg) {
		queueManager.enQueue(msg);
	}

	public void registerQueue(QueueHandler queue) {
		MessageQueue msgQueue = queueManager.getMsgQueue(queue.getDestination());
		if (msgQueue == null)
			queueManager.registerQueue(queue.getDestination(), queue.getMsgQueue());
		else
			queue.setMsgQueue(msgQueue);
	}

	public void registerQueue(MessageDestination msgDest, MessageQueue msgQueue) {
		queueManager.registerQueue(msgDest, msgQueue);
	}

	public void removeQueue(MessageDestination dest) {
		queueManager.removeQueue(dest);
	}

	public CommSystem getCommSystem() {
		return commSystem;
	}

	/**
	 * Get the queue for a given destination.
	 * 
	 * @param destination
	 *            The identifier for the desired queue
	 * @return The desired queue, or null if it doesn't exist
	 */
	public MessageQueue getQueue(MessageDestination destination) {
		return queueManager.getQueue(destination);
	}

	/**
	 * Get the advertisements in the broker.
	 * 
	 * @return The set of advertisements in the broker.
	 */
	public Map<String, AdvertisementMessage> getAdvertisements() {
		return router.getAdvertisements();
	}

	/**
	 * Get the subscriptions in the broker.
	 * 
	 * @return The set of subscriptions in the broker.
	 */
	public Map<String, SubscriptionMessage> getSubscriptions() {
		return router.getSubscriptions();
	}

	/**
	 * Retrieve the debug mode of this broker
	 * 
	 * @return Boolean value where true indicates debug mode
	 */
	public boolean getDebugMode() {
		return debug;
	}

	/**
	 * Set the debug mode of this broker
	 * 
	 * @param debugMode
	 *            True to set broker to debug mode, false to turn off debug mode
	 */
	public void setDebugMode(boolean debugMode) {
		debug = debugMode;
	}

	/**
	 * Returns the number of messages in the input queue
	 * 
	 * @return the number of messages in the input queue
	 */
	public int getInputQueueSize() {
		return inputQueue.getInputQueueSize();
	}

	/**
	 * Shuts down this broker along with all services under this broker
	 */
	public void shutdown() {
		
		if(isShutdown)
			return;
		
		isShutdown  = true;
        if (systemMonitor != null) {
            systemMonitor.shutdownBroker();
        }
		
		// Let's be nice
		try {
			stop();
			brokerCoreLogger.info("BrokerCore is shutting down.");
//			orderQueuesTo("SHUTDOWN");
			if (commSystem != null)
				commSystem.shutDown();
		} catch (CommunicationException e) {
			e.printStackTrace();
			exceptionLogger.error(e.getMessage());
		}
		if(controller != null) {
            controller.shutdown();
        }
        if(inputQueue != null) {
            inputQueue.shutdown();
        }
        if(timerThread != null) {
            timerThread.shutdown();
        }
        if(heartbeatPublisher != null) {
            heartbeatPublisher.shutdown();
        }
        if(heartbeatSubscriber != null) {
            heartbeatSubscriber.shutdown();
        }

        if (webuiMonitorToBeRemoved != null) {
            webuiMonitorToBeRemoved.shutdownBroker();
        }
        if(timerThread != null) {
            timerThread.shutdown();
        }
	}

	/**
	 * Stops all broker activity Publishers/Neighbours can still send messages to the brokercore
	 */
	public void stop() {
		// Stop all input/output queues from receiving messages.
		// NOTE: The input queue is never stopped or else there will be no way to start it up again
		// remotely
		try {
			brokerCoreLogger.info("BrokerCore is stopping.");
			orderQueuesTo("STOP");
			running = false;
		} catch (ParseException e) {
			e.printStackTrace();
			exceptionLogger.error(e.getMessage());
		}
	}

	/**
	 * Resumes all broker activity
	 * 
	 */
	public void resume() {
		// Allow messages to be delivered
		try {
			brokerCoreLogger.info("BrokerCore is resuming.");
			orderQueuesTo("RESUME");
			running = true;
		} catch (ParseException e) {
			e.printStackTrace();
			exceptionLogger.error(e.getMessage());
		}
	}

	/*
	 * Send a STOP, RESUME, or SHUTDOWN control message to the LifeCycle, Overlay Managers and System Monitor
	 */
	protected void orderQueuesTo(String command) throws ParseException {
		// Send a control message to the LifeCycle Manager
		Publication lcPub = MessageFactory.createPublicationFromString("[class,BROKER_CONTROL],[brokerID,'" + getBrokerID()
				+ "'],[command,'LIFECYCLE-" + command + "']");
		PublicationMessage lcPubmsg = new PublicationMessage(lcPub, getNewMessageID(),
				getBrokerDestination());
		brokerCoreLogger.debug("Command " + command + " is sending to LifecycleManager.");
		if (queueManager != null)
			queueManager.enQueue(lcPubmsg, MessageDestination.CONTROLLER);

		// Send a control message to the Overlay Manager
		Publication omPub = MessageFactory.createPublicationFromString("[class,BROKER_CONTROL],[brokerID,'" + getBrokerID()
				+ "'],[command,'OVERLAY-" + command + "']");
		PublicationMessage omPubmsg = new PublicationMessage(omPub, getNewMessageID(),
				getBrokerDestination());
		brokerCoreLogger.debug("Command " + command + " is sending to OverlayManager.");
		if (queueManager != null)
			queueManager.enQueue(omPubmsg, MessageDestination.CONTROLLER);
	}

	/**
	 * Indicates whether this broker is running or not
	 * 
	 * @return boolean value, true indicates the broker is running, false means the broker is
	 *         stopped.
	 */
	public boolean isRunning() {
		return running;
	}

	public CycleType getCycleOption() {
		return brokerConfig.getCycleOption();
	}

	public boolean isDynamicCycle() {
		return isDynamicCycle;
	}

	public boolean isCycle() {
		return isCycle;
	}

	public SystemMonitor getSystemMonitor() {
		if (systemMonitor == null) {
			System.err.println("Call to getSystemMonitor() before initializing the system monitor");
		}
		return systemMonitor;
	}

	public Controller getController() {
		return controller;
	}

	public OverlayManager getOverlayManager() {
		return controller == null ? null : controller.getOverlayManager();
	}

	public HeartbeatPublisher getHeartbeatPublisher() {
		return heartbeatPublisher;
	}

	public TimerThread getTimerThread() {
		return timerThread;
	}

	public Router getRouter() {
		return router;
	}

	public InputQueueHandler getInputQueue() {
		return inputQueue;
	}

	public static void main(String[] args) {
		try {
			BrokerCore brokerCore = new BrokerCore(args);
			brokerCore.initialize();
//			brokerCore.shutdown();
		} catch (Exception e) {
			// log the error the system error log file and exit
			Logger sysErrLogger = Logger.getLogger("SystemError");
			if (sysErrLogger != null)
				sysErrLogger.fatal(e.getMessage() + ": " + e);
			e.printStackTrace();
			System.exit(1);
		}
	}

	public boolean isShutdown() {
		return isShutdown;
	}

}
