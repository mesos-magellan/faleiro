package org.magellan.faleiro;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.WatchEvent;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;


public class LeaderElectionTest {
    private String leaderId = "001";
    private String follower1Id = "002";
    private String follower2Id = "003";
    private static boolean passed = false;

    ZookeeperService mockzk_leader = null;
    ZookeeperService mockzk_follower1 = null;
    ZookeeperService mockzk_follower2 = null;
    LeaderElection leader;
    LeaderElection follower1;
    LeaderElection follower2;

    @Before
    public void setUp() throws Exception {
        mockzk_leader = mock(ZookeeperService.class);
        mockzk_follower1 = mock(ZookeeperService.class);
        mockzk_follower2 = mock(ZookeeperService.class);
        leader = new LeaderElection(mockzk_leader);
        follower1 = new LeaderElection(mockzk_follower1);
        follower2 = new LeaderElection(mockzk_follower2);

        String elecRoot = leader.getElectionRoot();
        String childPrefix = leader.getChildNodePrefix();

        // This is what the children of the election root look like
        ArrayList<String> children = new ArrayList<String>();
        children.add(childPrefix.substring(childPrefix.lastIndexOf('/') + 1) + leaderId);
        children.add(childPrefix.substring(childPrefix.lastIndexOf('/') + 1) + follower1Id);
        children.add(childPrefix.substring(childPrefix.lastIndexOf('/') + 1) + follower2Id);

        // Mock what each "scheduler" is supposed to see when they make calls to createNode and
        // get children from zookeeper
        doReturn(elecRoot).when(mockzk_leader).createNode(eq(elecRoot),anyBoolean(),anyBoolean());
        doReturn(elecRoot+childPrefix+leaderId).when(mockzk_leader).createNode(eq(elecRoot + childPrefix),anyBoolean(),anyBoolean());
        doReturn(children).when(mockzk_leader).getChildren(eq(elecRoot),anyBoolean());
        doReturn(true).doThrow(new IllegalStateException()).when(mockzk_leader).watchNode(anyString(),anyBoolean());

        doReturn(elecRoot).when(mockzk_follower1).createNode(eq(elecRoot),anyBoolean(),anyBoolean());
        doReturn(elecRoot+childPrefix+follower1Id).when(mockzk_follower1).createNode(eq(elecRoot + childPrefix),anyBoolean(),anyBoolean());
        doReturn(children).when(mockzk_follower1).getChildren(eq(elecRoot),anyBoolean());
        doReturn(true).doThrow(new IllegalStateException()).when(mockzk_follower1).watchNode(anyString(),anyBoolean());

        doReturn(elecRoot).when(mockzk_follower2).createNode(eq(elecRoot),anyBoolean(),anyBoolean());
        doReturn(elecRoot+childPrefix+follower2Id).when(mockzk_follower2).createNode(eq(elecRoot + childPrefix),anyBoolean(),anyBoolean());
        doReturn(children).when(mockzk_follower2).getChildren(eq(elecRoot),anyBoolean());
        doReturn(true).doThrow(new IllegalStateException()).when(mockzk_follower2).watchNode(anyString(),anyBoolean());

        leader.initialize();
        follower1.initialize();
        follower2.initialize();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testInitialize() throws Exception {
        assertTrue(leader.isLeader());
        assertFalse(follower1.isLeader());
        assertFalse(follower2.isLeader());
    }

    @Test
    public void testBlockUntilElectedLeader() throws Exception {
        new Thread(){
            public void run(){
                follower1.blockUntilElectedLeader();
                // Should not execute as previous call should block since
                // follower1 blocks
                passed = true;
            }
        }.start();

        Thread.sleep(1000);
        assertFalse(passed);
    }

    @Test
    public void testNewLeader() throws Exception {
        String leaderPath = leader.getElectionRoot() + leader.getChildNodePrefix() + leaderId;
        String childPrefix = leader.getChildNodePrefix();

        // If the leader dies, this is what the children look like
        ArrayList<String> children = new ArrayList<String>();
        children.add(childPrefix.substring(childPrefix.lastIndexOf('/') + 1) + follower1Id);
        children.add(childPrefix.substring(childPrefix.lastIndexOf('/') + 1) + follower2Id);
        doReturn(children).when(mockzk_follower1).getChildren(eq(leader.getElectionRoot()),anyBoolean());

        //Event for leader went down
        WatchedEvent deleteEvent = new WatchedEvent(Watcher.Event.EventType.NodeDeleted, null, leaderPath);

        follower1.process(deleteEvent);
        assertTrue(follower1.isLeader());

    }

    @Test
    public void testGetWatchedNodePath() throws Exception {
        assertEquals(follower1.getWatchedNodePath(), follower1.getElectionRoot() + follower1.getChildNodePrefix() + leaderId);
        assertEquals(follower2.getWatchedNodePath(), follower1.getElectionRoot() + follower2.getChildNodePrefix() + follower1Id);
    }


}