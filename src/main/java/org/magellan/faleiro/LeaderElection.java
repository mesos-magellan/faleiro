package org.magellan.faleiro;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LeaderElection  implements Watcher, AsyncCallback.StatCallback {
    Object m_lock = new Object();
    Boolean isLeader = false;
    ZooKeeper mZk;

    public LeaderElection(ZooKeeper zk){
        mZk = zk;
    }

    public void connect(){

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
    public void processResult(int i, String s, Object o, Stat stat) {

    }

    @Override
    public void process(WatchedEvent watchedEvent) {

    }
}
