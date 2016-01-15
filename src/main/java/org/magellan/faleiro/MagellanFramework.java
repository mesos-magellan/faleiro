package org.magellan.faleiro;

import com.google.protobuf.ByteString;
import com.netflix.fenzo.*;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.VMLeaseObject;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class MagellanFramework {

    class MagellanScheduler implements Scheduler {

        public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
            System.out.println("Registered! ID = " + frameworkID.getValue());
            fenzoScheduler.expireAllLeases();
        }

        public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
            System.out.println("Re-registered " + masterInfo.getId());
            fenzoScheduler.expireAllLeases();
        }

        public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {
            for(Protos.Offer offer: offers) {
                System.out.println("Adding offer " + offer.getId() + " from host " + offer.getHostname());
                leasesQueue.offer(new VMLeaseObject(offer));
            }
        }

        public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {
            fenzoScheduler.expireLease(offerID.getValue());
        }

        public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
            System.out.println("Task Update: " + taskStatus.getTaskId().getValue() + " in state " + taskStatus.getState());
            switch (taskStatus.getState()) {
                case TASK_FAILED:
                case TASK_LOST:
                case TASK_FINISHED:
                    // TODO: Parse data from executor to figure out result of the SA to determine starting point of next task
                    fenzoScheduler.getTaskUnAssigner().call(taskStatus.getTaskId().getValue(), launchedTasks.get(taskStatus.getTaskId().getValue()));
                    break;
            }
        }

        public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {

        }

        public void disconnected(SchedulerDriver schedulerDriver) {

        }

        public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {
            fenzoScheduler.expireAllLeasesByVMId(slaveID.getValue());
        }

        public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {

        }

        public void error(SchedulerDriver schedulerDriver, String s) {

        }
    }

    private final TaskScheduler fenzoScheduler;
    private final AtomicReference<MesosSchedulerDriver> mesosDriverReference = new AtomicReference<>();
    private final MesosSchedulerDriver mesosSchedulerDriver;
    private final AtomicBoolean isFrameworkShutdown = new AtomicBoolean(false);
    private final ConcurrentHashMap<Long, MagellanJob> jobsList = new ConcurrentHashMap<>();
    private final BlockingQueue<VirtualMachineLease> leasesQueue = new LinkedBlockingQueue<>();

    private  long numCreatedJobs = 0;
    private final Map<String, String> launchedTasks = new HashMap<>();

    public MagellanFramework(String mesosMasterIP){
        fenzoScheduler = new TaskScheduler.Builder()
                .withLeaseOfferExpirySecs(1000000000)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    public void call(VirtualMachineLease lease) {
                        System.out.println("Declining offer on " + lease.hostname());
                        mesosDriverReference.get().declineOffer(lease.getOffer().getId());
                    }
                })
                .build();

        Scheduler mesosScheduler = new MagellanScheduler();

        Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder()
                .setUser(System.getenv("FRAMEWORK_USER")) //Have Mesos fill in current user
                .setName("Number Search Framework")
                .setPrincipal(System.getenv("PRINCIPAL"));


        if (System.getenv("MESOS_AUTHENTICATE") != null) {
            System.out.println("Enabling authentication for the framework");
            System.out.println(System.getenv("SECRET"));
            System.out.println(System.getenv("PRINCIPAL"));

            if (System.getenv("PRINCIPAL") == null) {
                System.err.println("Expecting authentication principal in the environment");
                System.exit(1);
            }

            Protos.Credential.Builder credentialBuilder = Protos.Credential.newBuilder()
                    .setPrincipal(System.getenv("PRINCIPAL"));

            if (System.getenv("SECRET") != null) {
                credentialBuilder.setSecret(ByteString.copyFrom(System.getenv("SECRET").getBytes()));
            }

            frameworkBuilder.setPrincipal(System.getenv("PRINCIPAL"));

            mesosSchedulerDriver = new MesosSchedulerDriver(
                    mesosScheduler,
                    frameworkBuilder.build(),
                    mesosMasterIP,
                    true,
                    credentialBuilder.build());
        } else {
            frameworkBuilder.setPrincipal("java-number_scheduler");
            mesosSchedulerDriver = new MesosSchedulerDriver(
                    mesosScheduler,
                    frameworkBuilder.build(),
                    mesosMasterIP,
                    true);
        }
        mesosDriverReference.set(mesosSchedulerDriver);

        new Thread() {
            public void run() {
                mesosSchedulerDriver.run();
            }
        }.start();                ;
    }

    public void shutdownFramework() {
        System.out.println("Shutting down mesos driver");
        Protos.Status status = mesosSchedulerDriver.stop();
        isFrameworkShutdown.set(true);
    }

    public void startFramework(){
        new Thread(() -> {
            runFramework();
        }).start();
    }

    public long createJob() {
        long id = numCreatedJobs++;
        MagellanJob j = new MagellanJob(id);
        jobsList.put(id, j);
        j.start();

        return id;
    }

    public void stopJob(Long jobID) {
        MagellanJob j = jobsList.get(jobID);
        if(j!=null){
            j.stop();
        }
    }

    public void pauseJob(Long jobID) {
        MagellanJob j = jobsList.get(jobID);
        if(j!=null){
            j.pause();
        }
    }

    public void resumeJob(Long jobID){
        MagellanJob j = jobsList.get(jobID);
        if(j!=null){
            j.resume();
        }
    }

    /**
     *  This contains the main loop of the program. In here, the framework queries
     *  each running job in the system to get a list of tasks each job wants to run.
     *  These set of tasks are then given to Fenzo which matches availble resource
     *  offers from Mesos to tasks.The resulting matches from Fenzo are then given
     *  to the Mesos Driver for execution.
     */
    public void runFramework(){

    }
}
