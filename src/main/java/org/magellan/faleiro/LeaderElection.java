package org.magellan.faleiro;

import org.apache.zookeeper.*;
import java.util.*;

public class LeaderElection  implements Watcher{
    private final String LEADER_ELECTION_ROOT_NODE = "/election";
    private final String CHILD_NODE_PREFIX = "/p_";

    private Object m_lock = new Object();
    private Boolean m_isLeader = false;
    private ZookeeperService m_zK;
    private String m_childNodePath;
    private String m_watchedNodePath;

    public LeaderElection(ZookeeperService zk){
        m_zK = zk;
    }

    /**
     * Creates the root node for leader election if it doesn't exist
     */
    public void initialize(){
        final String rootNodePath = m_zK.createNode(LEADER_ELECTION_ROOT_NODE, false, false);
        m_childNodePath = m_zK.createNode(rootNodePath + CHILD_NODE_PREFIX, false, true);
        attemptForLeaderPosition();
    }

    /**
     * This method is triggered either when the immediate predecessor of this node goes
     * down. WHen this happens, we need to check if the node that went down is the leader.
     * If it is, we become the new leader. If not, then we set another watcher for our next
     * predecessor
     */
    private void attemptForLeaderPosition() {

        final List<String> childNodePaths = m_zK.getChildren(LEADER_ELECTION_ROOT_NODE, false);

        Collections.sort(childNodePaths);

        int index = childNodePaths.indexOf(m_childNodePath.substring(m_childNodePath.lastIndexOf('/') + 1));
        if(index == 0) {
            System.out.println("[LEADER ELECTION] - THIS SCHEDULER HAS JUST BEEN ELECTED LEADER!");
            synchronized (m_lock) {
                m_isLeader = true;
                m_lock.notify();
            }
        } else {
            // Someone else is elected leader so set a watcher on the node before you to get notified when it dies
            final String watchedNodeShortPath = childNodePaths.get(index - 1);
            m_watchedNodePath = LEADER_ELECTION_ROOT_NODE + "/" + watchedNodeShortPath;
            System.out.println("[LEADER ELECTION] - SETTING WATCH ON NODE WITH PATH: " + m_watchedNodePath);
            m_zK.watchNode(m_watchedNodePath, true);
        }
    }


    /**
     * This method blocks until the current scheduler becomes the leader
     */
    public void blockUntilElectedLeader(){
        synchronized (m_lock){
            try {
                while(!m_isLeader) {
                    System.out.println("[LEADER ELECTION] - WAITING UNTIL ELECTED LEADER");
                    m_lock.wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * This callback function is called whenever a watched event is triggered
     * in zookeeper. In this case, a watched event is triggered when
     * the current nodes immediate successor dies. This node can either be
     * the Leader or another scheduler.When this callback is called, we attempt
     * to become the leader
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        System.out.println("[LEADER ELECTION] - EVENT RECEIVED: " + event);
        final Event.EventType eventType = event.getType();
        if(Event.EventType.NodeDeleted.equals(eventType)) {
            if(event.getPath().equalsIgnoreCase(m_watchedNodePath)) {
                attemptForLeaderPosition();
            }
        }

    }
}
