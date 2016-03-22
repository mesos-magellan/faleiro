package org.magellan.faleiro;

import io.atomix.Atomix;
import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.group.DistributedGroup;
import io.atomix.group.LocalGroupMember;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LeaderElection {
    Object m_lock = new Object();
    Boolean isLeader = false;
    Address localAddr;
    List<Address> members;

    public LeaderElection(Address localAddr, List<Address> groupMembers){
        this.localAddr = localAddr;
        this.members = groupMembers;
    }

    public void connect(){
        try {

            // Create a stateful Atomix replica. The replica communicates with other replicas in the cluster
            // to replicate state changes.
            Atomix atomix = AtomixReplica.builder(localAddr, members)
                    .withTransport(new NettyTransport())
                    .withStorage(new Storage(StorageLevel.MEMORY))
                    .build();

            // Open the replica. Once this operation completes resources can be created and managed.
            atomix.open().join();

            System.out.println("Joined");

            // Create a leader election resource.
            DistributedGroup group = atomix.getGroup("election").get();
            System.out.println("Got group");

            // Join the group.
            LocalGroupMember member = group.join().get();
            System.out.println("Joined local something");

            // Register a callback to be called when the local member is elected the leader.
            group.election().onElection(leader -> {
                if (leader.equals(member)) {
                    synchronized (m_lock) {
                        System.out.println("Elected leader!");
                        isLeader = true;
                        m_lock.notify();
                    }
                }
            });

            // Block while the replica is open.
            /*while (atomix.isOpen()) {
                Thread.sleep(1000);
            }*/
        }catch (InterruptedException e){
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
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

}
