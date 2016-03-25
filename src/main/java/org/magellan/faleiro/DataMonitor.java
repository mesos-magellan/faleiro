package org.magellan.faleiro;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.json.JSONException;
import org.json.JSONObject;
import org.apache.zookeeper.KeeperException.Code;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataMonitor implements Watcher{

    private ZookeeperService m_zk;

    final public int WRITE_DELAY = 5000;

    private String m_znode;

    private byte prevData[];

    private JSONObject initialState = null;

    private MagellanFramework mframework;

    public DataMonitor(ZookeeperService zk, String znode, MagellanFramework framework) {
        this.m_zk = zk;
        this.m_znode = znode;
        this.mframework = framework;
    }

    public void initialize(){
        try {

            final String node = m_zk.createNode(m_znode, false, false);
            byte[] retrievedState = m_zk.getData(m_znode);

            if(retrievedState!=null) {
                try {
                    initialState = new JSONObject(new String(retrievedState, "US-ASCII"));
                    prevData = initialState.toString().getBytes("UTF-8");
                    System.out.println("[DATA MONITOR] - DISCOVERED PREVIOUS STATE");
                }catch (JSONException e){
                    initialState = null;
                    prevData = null;
                }
            }

            // Create a thread that will write the system state of the magellan framework
            // every couple seconds to zookeeper.
            new Thread(){
                public void run(){
                    while(true) {
                        try {
                            Thread.sleep(WRITE_DELAY);
                            writeState(mframework.getVerboseSystemInfo());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }.start();

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns initial state stored in Zookeeper nodes during inititalizion of scheduler
     * @return      JSONObject : If state was stored and retrieved
     *              null       : No state was stored
     */
    public JSONObject getInitialState(){
        return initialState;
    }

    /**
     * Returns the state that was last recorded onto Zookeeper
     * @return      JSONobject :  State was successfully decoded from byte array to JSONObject
     *              null       :  Something may exist in the Zookeeper nodes but it cant be translated
     *                            into a JSONObject.
     */
    public JSONObject getLatestStoredState(){
        if(prevData == null){
            return null;
        }

        try {
            return new JSONObject(new String(prevData, "US-ASCII"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Synchronous method that writes state to the Zookeeper node if it is different
     * than the last state written to the node. If it is the same, or if state is
     * null, than the method returns without doing anything.
     *
     * Note
     * @param state    JSONObject : state to be written
     */
    public void writeState(JSONObject state) {
        if (state == null){
            return;
        }

        try {
            byte newData[] = state.toString().getBytes("UTF-8");

            // Record changes only if the state has changed
            if(prevData == null || !Arrays.equals(prevData, newData)) {
                System.out.println("[DATA MONITOR] - PERSISTING STATE TO ZOOKEEPER");
                m_zk.setData(m_znode, newData);
                prevData = newData;
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("[DATA MONITOR] - EVENT RECEIVED: " + event);
    }
}
