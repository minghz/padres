/*
 * Created on Apr 24, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package ca.utoronto.msrg.padres.components.scout;
import org.junit.Before;

import org.junit.Test;

import org.junit.Assert;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

import ca.utoronto.msrg.padres.broker.brokercore.BrokerConfig;
import ca.utoronto.msrg.padres.broker.router.scout.*;
import ca.utoronto.msrg.padres.common.message.*;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;

/**
 * @author cheung
 * 
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public class ScoutTestRemoveDupChildBug extends Assert {

	private static final String subscriptionFile = BrokerConfig.PADRES_HOME
			+ "/etc/test/junit/matching/scout/ScoutTestRemoveDupChild.txt";

	private Scout scout;

	Map<String, Message> idToMsgMap;

	/**
	 * Constructor for ScoutTestRemoveDupChildBug.
	 * 
	 * @param arg0
	 */
	public ScoutTestRemoveDupChildBug(String arg0) {
		scout = new Scout();
		idToMsgMap = new HashMap<String, Message>(6);
	}

	/*
	 * @see TestCase#setUp()
	 */
   @Before
   public void setUp() throws Exception {

		loadScout();
	}

	private void loadScout() {
		int id = 0;
		String line;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(subscriptionFile));
			while ((line = reader.readLine()) != null) {
				String idStr = Integer.toString(id++);
				Subscription sub = MessageFactory.createSubscriptionFromString(line);
				sub.setSubscriptionID(idStr);
				SubscriptionMessage subMsg = new SubscriptionMessage(sub, idStr, null);
				idToMsgMap.put(idStr, subMsg);
				scout.insert(subMsg);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		// scout.showTree();
	}

   @Test
	public void testRemoveDupChildBug() {
		String idToRemove = "1";
		String invalidID = "2";
		scout.remove(idToRemove);
		assertTrue(!scout.coveringSubscriptionSet().contains(idToMsgMap.get(invalidID)));
		// scout.showTree();
	}
}
