package org.magellan.faleiro;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;


public class ZookeeperServiceTest {
    ZooKeeper zk = null;
    ZookeeperService zks = null;

    @Before
    public void setUp() throws Exception {
        zk = mock(ZooKeeper.class);
        zks  = new ZookeeperService(zk);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testCreateNode() throws Exception {
        // NodE doesn't exist
        doReturn(null).when(zk).exists(anyString(),anyBoolean());
        assertEquals(null,zks.createNode("test",false,false));

        // Node exists
        Stat s = new Stat();
        doReturn(s).when(zk).exists(anyString(),anyBoolean());
        doReturn("test").when(zk).create(anyString(),any(byte[].class),eq(ZooDefs.Ids.OPEN_ACL_UNSAFE),any(CreateMode.class));
        assertEquals("test",zks.createNode("test",false,false));
    }

    @Test
    public void testWatchNode() throws Exception {

        // Node doesn't exist
        doReturn(null).when(zk).exists(anyString(),anyBoolean());
        assertFalse(zks.watchNode("",true));

        //Node exists
        doReturn(new Stat()).when(zk).exists(anyString(),anyBoolean());
        assertTrue(zks.watchNode("",true));


        //Throw exception
        when(zk.exists(anyString(),anyBoolean())).thenThrow(new InterruptedException());
        try {
            zks.watchNode("",true);
            assertFalse(true);
        }catch (IllegalStateException e){
            assertTrue(true);
        }
    }

    @Test
    public void testGetChildren() throws Exception {
        List<String> childnodes = new ArrayList<>();
        childnodes.add("p_001");
        childnodes.add("P_002");
        doReturn(childnodes).when(zk).getChildren(anyString(),anyBoolean());
        assertArrayEquals(childnodes.toArray(),zk.getChildren("",false).toArray());

        //Throw exception
        when(zk.getChildren(anyString(),anyBoolean())).thenThrow(new InterruptedException());
        try {
            zks.getChildren("",true);
            assertFalse(true);
        }catch (IllegalStateException e){
            assertTrue(true);
        }

    }

    @Test
    public void testGetData() throws Exception {
        // Node doesn't exist
        doReturn(null).when(zk).exists(anyString(),anyBoolean());
        assertEquals(null,zks.getData("test"));

        // Node exists
        Stat s = new Stat();
        doReturn(s).when(zk).exists(anyString(),anyBoolean());
        doReturn("test".getBytes()).when(zk).getData(anyString(),anyBoolean(),eq(null));
        assertArrayEquals("test".getBytes(),zks.getData(""));

        //Throw exception
        when(zk.getData(anyString(),anyBoolean(),eq(null))).thenThrow(new InterruptedException());
        assertNull(zks.getData(""));

    }

    @Test
    public void testSetData() throws Exception {
        doReturn(new Stat()).when(zk).setData(anyString(),any(byte[].class),eq(-1));
        zks.setData("",new byte[0]);
        assertTrue(true);

    }
}