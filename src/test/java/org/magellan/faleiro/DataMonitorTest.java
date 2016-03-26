package org.magellan.faleiro;

import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.xml.crypto.Data;
import java.util.ArrayList;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class DataMonitorTest {
    ZookeeperService zks = null;
    MagellanFramework mf = null;
    DataMonitor dm;
    
    @Before
    public void setUp() throws Exception {
        zks = mock(ZookeeperService.class);
        mf = mock(MagellanFramework.class);
        doReturn("").when(zks).createNode(anyString(),anyBoolean(),anyBoolean());
        doReturn(new JSONObject()).when(mf).getVerboseSystemInfo();
        doNothing().when(zks).setData(anyString(),anyObject());
        dm = new DataMonitor(zks,"",mf);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testInitialize() throws Exception {
        doReturn("").when(zks).createNode(anyString(),anyBoolean(),anyBoolean());

        JSONObject test = new JSONObject();
        test.put("test","test1");

        //Test null response
        doReturn(null).when(zks).getData(anyString());
        dm.initialize();
        assertEquals(dm.getInitialState(), null);

        //Test invalid Json
        doReturn(new byte[0]).when(zks).getData(anyString());
        dm.initialize();
        assertEquals(dm.getInitialState(), null);

        //Test valid Json
        doReturn(test.toString().getBytes()).when(zks).getData(anyString());
        dm.initialize();
        assertEquals(dm.getInitialState().get("test"), test.get("test"));

    }


    @Test
    public void testPersist(){
        doReturn(null).when(mf).getVerboseSystemInfo();
        dm.persistState();

        //Test null response
        assertFalse(dm.writeState(null));

        //Write new data
        assertTrue(dm.writeState(new JSONObject().put("test1","test1")));

        // Pass data same as data in zookeeper. this shouldn't write
        // Test valid Json
        JSONObject test = new JSONObject();
        test.put("test","test1");
        doReturn(test.toString().getBytes()).when(zks).getData(anyString());
        dm.initialize();
        assertFalse(dm.writeState(test));
    }

    @Test
    public void testLatestStoredState() {
        assertEquals(dm.getLatestStoredState(),null);

        JSONObject test = new JSONObject();
        test.put("test","test1");
        doReturn(test.toString().getBytes()).when(zks).getData(anyString());
        dm.initialize();
        assertTrue(dm.getLatestStoredState().has("test"));
        assertEquals(dm.getLatestStoredState().get("test"),test.get("test"));

    }

    @Test
    public void testProcess(){
        dm.process(null);
    }
}