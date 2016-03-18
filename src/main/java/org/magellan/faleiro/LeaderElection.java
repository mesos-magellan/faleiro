package org.magellan.faleiro;

import io.atomix.Atomix;
import io.atomix.AtomixReplica;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.copycat.server.storage.Storage;
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

    public LeaderElection(String currentAddr, int currentPort, Map<String, String> groupMembers){

        try {
            Address localAddr = new Address(currentAddr, currentPort);

            // Build a list of all member addresses to which to connect.
            List<Address> members = new ArrayList<>();
            for (Map.Entry<String, String> entry : groupMembers.entrySet()) {
                members.add(new Address(entry.getKey(), Integer.valueOf(entry.getValue())));
            }

            // Create a stateful Atomix replica. The replica communicates with other replicas in the cluster
            // to replicate state changes.
            Atomix atomix = AtomixReplica.builder(localAddr, members)
                    .withTransport(new NettyTransport())
                    .withStorage(new Storage(System.getenv("ATOMIX_LOGS_DIR")))
                    .build();

            // Open the replica. Once this operation completes resources can be created and managed.
            atomix.open().join();

            // Create a leader election resource.
            DistributedGroup group = atomix.getGroup("group").get();

            // Join the group.
            LocalGroupMember member = group.join().get();

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
            while (atomix.isOpen()) {
                Thread.sleep(1000);
            }
        }catch (InterruptedException e){
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }


    public void blockUntilElectedLeader(){
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
