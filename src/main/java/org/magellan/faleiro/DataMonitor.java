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

public class DataMonitor implements Watcher, AsyncCallback.StatCallback {

    ZooKeeper zk;

    final public int WRITE_DELAY = 5000;

    // Absolute path to znode on Zookeeper service
    String znode;

    Watcher chainedWatcher;

    boolean dead;

    DataMonitorListener listener;

    byte prevData[];

    JSONObject initialState;

    MagellanFramework mframework;

    public DataMonitor(ZooKeeper zk, String znode, Watcher chainedWatcher,
                       DataMonitorListener listener, MagellanFramework framework) {
        this.zk = zk;
        this.znode = znode;
        this.chainedWatcher = this;
        this.listener = listener;
        this.mframework = framework;

        try {
            // Perform a synchronous call to the Zookeeper service to check if
            // the znode exists
            Stat st = zk.exists(znode, true);

            if(st != null){
                System.out.println("Status is " + st.toString());
                // This means that the znode exists and might have state that we need to
                // read back.
                // Next step is to synchronously retrieve this data from the znode, convert
                // it to the right format and return this
                byte[] retrievedState = zk.getData(znode,true, null);

                if(retrievedState!=null) {
                    try {
                        initialState = new JSONObject(new String(retrievedState, "US-ASCII"));
                        prevData = initialState.toString().getBytes("UTF-8");
                    }catch (JSONException e){
                        initialState = null;
                        prevData = null;
                    }
                }

            } else {
                System.out.println("Status is null");
                // This znode doesn't exist so create it.
                String p = zk.create(znode,new JSONObject().toString().getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                System.out.println("Create path is " + p);

            }

            // Start a thread that periodically sends system state to Zookeeper
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
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
     * Asynchronous method that writes state to the Zookeeper node if it is different
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
                System.out.println("Writing updated state to zookeeper nodes");
                zk.setData(znode, newData, -1, this, null);
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    /**
     * Other classes use the DataMonitor by implementing this method
     */
    public interface DataMonitorListener {
        /**
         * The existence status of the node has changed.
         */
        void exists(byte data[]);

        /**
         * The ZooKeeper session is no longer valid.
         *
         * @param rc
         *                the ZooKeeper reason code
         */
        void closing(Code rc);
    }

    /**
     * Async callback function called when a watched event goes off. Eg. znode data changes
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        String path = event.getPath();
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
            switch (event.getState()) {
                case SyncConnected:
                    // In this particular example we don't need to do anything
                    // here - watches are automatically re-registered with
                    // server and any watches triggered while the client was
                    // disconnected will be delivered (in order of course)
                    break;
                case Expired:
                    // It's all over
                    dead = true;
                    if(listener!=null) {
                        listener.closing(Code.SESSIONEXPIRED);
                    }
                    break;
            }
        } else {
            if (path != null && path.equals(znode)) {
                // Something has changed on the node, let's find out
                zk.exists(znode, true, this, null);
            }
        }
    }

    @Override
    /**
     * Callback function called by AsyncCallback.
     */
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        System.out.println("RC is " +rc + ". Path is " + path);
        boolean exists;
        switch (Code.get(rc)) {
            case OK:
                exists = true;
                break;
            case NONODE:
                exists = false;
                break;
            case SESSIONEXPIRED:
            case NOAUTH:
                dead = true;
                if(listener!=null) {
                    listener.closing(Code.get(rc));
                }
                return;
            default:
                // Retry errors
                zk.exists(znode, true, this, null);
                return;
        }

        byte b[] = null;
        if (exists) {
            try {
                b = zk.getData(znode, false, null);
            } catch (KeeperException e) {
                // We don't need to worry about recovering now. The watch
                // callbacks will kick off any exception handling
                e.printStackTrace();
            } catch (InterruptedException e) {
                return;
            }
        }
        if ((b == null && b != prevData)
                || (b != null && !Arrays.equals(prevData, b))) {
            if(listener!=null) {
                listener.exists(b);
            }
            prevData = b;
        }
    }

}
