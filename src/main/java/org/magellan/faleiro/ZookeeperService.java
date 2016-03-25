package org.magellan.faleiro;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class ZookeeperService {
    private ZooKeeper zooKeeper;

    public ZookeeperService(final String url, final Watcher processNodeWatcher) throws IOException {
        try {
            zooKeeper = new ZooKeeper(url, 10000, processNodeWatcher);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String createNode(final String node, final boolean watch, final boolean ephimeral) {
        String createdNodePath = null;
        try {

            final Stat nodeStat =  zooKeeper.exists(node, watch);

            if(nodeStat == null) {
                createdNodePath = zooKeeper.create(node,
                                                    new JSONObject().toString().getBytes("UTF-8"),
                                                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                                    (ephimeral ?  CreateMode.EPHEMERAL_SEQUENTIAL : CreateMode.PERSISTENT));
            } else {
                createdNodePath = node;
            }

        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return createdNodePath;
    }

    public boolean watchNode(final String node, final boolean watch) {

        boolean watched = false;
        try {
            final Stat nodeStat =  zooKeeper.exists(node, watch);

            if(nodeStat != null) {
                watched = true;
            }

        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }

        return watched;
    }

    public List<String> getChildren(final String node, final boolean watch) {

        List<String> childNodes = null;

        try {
            childNodes = zooKeeper.getChildren(node, watch);
        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }

        return childNodes;
    }

}
