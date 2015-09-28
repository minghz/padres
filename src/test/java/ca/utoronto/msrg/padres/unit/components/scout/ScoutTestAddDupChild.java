/*
 * Created on Apr 24, 2005
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package ca.utoronto.msrg.padres.unit.components.scout;

import org.junit.Before;
import org.junit.Test;

import org.junit.Assert;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

import ca.utoronto.msrg.padres.broker.brokercore.BrokerConfig;
import ca.utoronto.msrg.padres.broker.router.scout.Scout;
import ca.utoronto.msrg.padres.broker.router.scout.ScoutNode;
import ca.utoronto.msrg.padres.common.message.*;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;

/**
 * @author cheung
 *         <p>
 *         Tests to see that a newly inserted node has to search for its children to add to its
 *         children set. If this is not done, then the supposedly child node can become a neighbor
 *         of its parent when the child's last parent is removed
 */
public class ScoutTestAddDupChild extends Assert {

    private static final String subscriptionFile = BrokerConfig.PADRES_HOME
            + "/etc/test/junit/matching/scout/ScoutTestAddDupChild.txt";

    private Scout scout;


    @Before
    public void setUp() {
        scout = new Scout();
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
                scout.insert(subMsg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // scout.showTree();
    }

    @Test
    public void testSupersetSubset() {
        Set<ScoutNode> expectedParentSet = new HashSet<ScoutNode>();
        expectedParentSet.add(scout.getNode("77"));
        expectedParentSet.add(scout.getNode("367"));
//		assertTrue(scout.getNode("49").parentSet.containsAll(expectedParentSet));
        assertTrue(expectedParentSet.containsAll(scout.getNode("49").parentSet));

    }
}
