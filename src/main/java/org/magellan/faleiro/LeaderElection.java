package org.magellan.faleiro;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class LeaderElection  implements Watcher{
    private Object m_lock = new Object();
    private Boolean isLeader = false;
    private ZookeeperService mZk;
    private final String LEADER_ELECTION_ROOT_NODE = "/election";
    private final String CHILD_NODE_PREFIX = "/p_";
    private String childNodePath;
    private String watchedNodePath;

    public LeaderElection(ZookeeperService zk){
        mZk = zk;
    }

    public void connect(){
        final String rootNodePath = mZk.createNode(LEADER_ELECTION_ROOT_NODE, false, false);
        childNodePath = mZk.createNode(rootNodePath + CHILD_NODE_PREFIX, false, true);
        attemptForLeaderPosition();
    }

    private void attemptForLeaderPosition() {

        final List<String> childNodePaths = mZk.getChildren(LEADER_ELECTION_ROOT_NODE, false);

        Collections.sort(childNodePaths);

        int index = childNodePaths.indexOf(childNodePath.substring(childNodePath.lastIndexOf('/') + 1));
        if(index == 0) {
           System.out.println("ELECTED LEADER!");
            synchronized (m_lock) {
                isLeader = true;
                m_lock.notify();
            }
        } else {
            // Someone else is elected leader so set a watcher on the node before you to get notified when it dies
            final String watchedNodeShortPath = childNodePaths.get(index - 1);

            watchedNodePath = LEADER_ELECTION_ROOT_NODE + "/" + watchedNodeShortPath;

           System.out.println("Setting watch on node with path: " + watchedNodePath);
            mZk.watchNode(watchedNodePath, true);
        }
    }


    public void blockUntilElectedLeader(){
        System.out.println("Waiting until elected leader");
        synchronized (m_lock){
            try {
                while(!isLeader) {
                    m_lock.wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("Event received " + event);
        final Event.EventType eventType = event.getType();
        if(Event.EventType.NodeDeleted.equals(eventType)) {
            if(event.getPath().equalsIgnoreCase(watchedNodePath)) {
                attemptForLeaderPosition();
            }
        }

    }
}
