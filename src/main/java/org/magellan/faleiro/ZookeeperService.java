package org.magellan.faleiro;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZookeeperService {
    private ZooKeeper zooKeeper;
    private static final Logger log = Logger.getLogger(ZookeeperService.class.getName());

    public ZookeeperService(ZooKeeper zooKeeper) throws IOException {
        this.zooKeeper = zooKeeper;
    }

    /**
     * Synchronously creates a new node in zookeeper. If the node already exists, nothing happens
     * @param node : The path of the node to create
     * @param watch : if true, a watched event will be placed on this newly created node
     * @param ephimeral : if true, this node will be ephimeral which means that the node will go down
     *                    when the scheduler goes down.
     * @return the path to the new node.
     *          null if an exception occurs
     */
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
            log.log(Level.SEVERE, e.getMessage());
            throw new IllegalStateException(e);
        } catch (UnsupportedEncodingException e) {
            log.log(Level.SEVERE, e.getMessage());
        }

        return createdNodePath;
    }

    /**
     * Places a watcher on a node if that node exists. If an event happens on the node,
     * a calllback function will be triggered.
     * @param node : path of node to watch
     * @param watch : if true, a watcher is placed on the node
     * @return parameter watch
     */
    public boolean watchNode(final String node, final boolean watch) {

        boolean watched = false;
        try {
            final Stat nodeStat =  zooKeeper.exists(node, watch);

            if(nodeStat != null) {
                watched = true;
            }

        } catch (KeeperException | InterruptedException e) {
            log.log(Level.SEVERE, e.getMessage());
            throw new IllegalStateException(e);
        }

        return watched;
    }

    /**
     * Returns the children of a root node
     * @param node : path to root node
     * @param watch : if true, places a watcher on the root node
     * @return list of children for the node passed in
     */
    public List<String> getChildren(final String node, final boolean watch) {
        List<String> childNodes = null;
        try {
            childNodes = zooKeeper.getChildren(node, watch);
        } catch (KeeperException | InterruptedException e) {
            log.log(Level.SEVERE, e.getMessage());
            throw new IllegalStateException(e);
        }

        return childNodes;
    }

    /**
     * Synchonrous call to retrieve data from a node
     * @param node
     * @return byte array stored at the node provided
     *          null if exception occurs
     */
    public byte[] getData(final String node){
        try {
            final Stat nodeStat = zooKeeper.exists(node, false);

            if (nodeStat != null) {
                return zooKeeper.getData(node, false, null);
            }
        }catch (InterruptedException e) {
            log.log(Level.SEVERE, e.getMessage());
        } catch (KeeperException e) {
            log.log(Level.SEVERE, e.getMessage());
        }
        return null;
    }

    /**
     * Synchronous call to write data at a node
     * @param node : Path of node that data needs to be written too
     * @param data : Byte array data to be written
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void setData(final String node, byte[] data) throws KeeperException, InterruptedException {
        zooKeeper.setData(node, data, -1);
    }

}
