package org.magellan.faleiro;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class DataMonitor implements Watcher, AsyncCallback.StatCallback {

    ZooKeeper zk;

    // Absolute path to znode on Zookeeper service
    String znode;

    Watcher chainedWatcher;

    boolean dead;

    DataMonitorListener listener;

    byte prevData[];

    JSONObject previousState;

    public DataMonitor(ZooKeeper zk, String znode, Watcher chainedWatcher,
                       DataMonitorListener listener) {
        this.zk = zk;
        this.znode = znode;
        this.chainedWatcher = this;
        this.listener = listener;

        System.out.println("Lets do this " + znode);

        try {
            // Perform a synchronous call to the Zookeeper service to check if
            // the znode exists
            Stat st = zk.exists(znode, true);
            System.out.println("Status is " + st);

            if(st != null){
                System.out.println("Status is " + st.toString());
                // This means that the znode exists and might have state that we need to
                // read back.
                // Next step is to synchronously retrieve this data from the znode, convert
                // it to the right format and return this
                byte[] retrievedState = zk.getData(znode,true, null);
                previousState = new JSONObject(new String(retrievedState,"US-ASCII"));

            } else {
                System.out.println("Status is null");
                // This znode doesn't exist so create it.
                String p = zk.create(znode,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                System.out.println("Create path is " + p);

            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public JSONObject getPreviousState(){


        return null;
    }

    public void persistState(JSONObject state){
        try {
            byte st[] = state.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processResult(int i, String s, Object o, Stat stat) {

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
        void closing(int rc);
    }


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
                    listener.closing(KeeperException.Code.SessionExpired);
                    break;
            }
        } else {
            if (path != null && path.equals(znode)) {
                // Something has changed on the node, let's find out
                zk.exists(znode, true, this, null);
            }
        }
    }

}
