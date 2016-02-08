package org.magellan.faleiro;

import com.google.protobuf.ByteString;
import com.netflix.fenzo.*;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.VMLeaseObject;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.json.JSONObject;

import java.util.*;
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
                    // Find which job this task is associated with at forward the message to it
                    String taskID = recoverTaskId(taskStatus.getData());
                    jobsList.get(submittedTaskIdsToJobIds.get(taskID)).processIncomingMessages(taskStatus.getData());
                    submittedTaskIdsToJobIds.remove(taskID);
                    taskIdsToTaskData.remove(taskID);

                    //Notify Fenzo that the task has completed and is no longer assigned
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
    private final Map<String, MagellanTaskRequest> pendingTasksMap = new HashMap<>();
    private final HashMap<String, Long> submittedTaskIdsToJobIds = new HashMap<>();
    private final HashMap<String, ByteString> taskIdsToTaskData = new HashMap<>();

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
                .setUser(System.getenv("FRAMEWORK_USER"))
                .setName("Simulated Annealing Scheduler")
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
            frameworkBuilder.setPrincipal("simulated annealing scheduler");
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
        }.start();
        mesosSchedulerDriver.stop();
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

    /**
     *
     * @param jName Name of job
     * @param jStartingTemp Starting temperature of job. Higher means job runs for longer
     * @param jCoolingRate Rate at which temperature depreciates each time
     * @param jCount number of iterations per temperature for job
     * @param tTemp Starting temperature of job
     * @param tCoolingRate Cooling rate of task
     * @param tCount Number of iterations per temperature for each task
     * @param pathToExecutor Path to the executor file on local machine
     */
    public long createJob(String jName, int jStartingTemp, double jCoolingRate, int jCount, double tTemp, double tCoolingRate, double tCount, String pathToExecutor) {
        long id = numCreatedJobs++;
        MagellanJob j = new MagellanJob(id, jName, jStartingTemp, jCoolingRate,jCount, tTemp, tCoolingRate, tCount,pathToExecutor);
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
        System.out.println("Running all");
        List<VirtualMachineLease> newLeases = new ArrayList<>();
        List<TaskRequest> newTaskRequests = new ArrayList<>();

        while(true) {
            // Only if the framework has shutdown do we exist our main loop
            if(isFrameworkShutdown.get()) {
                System.out.println("Framework terminated");
                return;
            }

            // Clear all the local data structures in preparation of a new loop
            newLeases.clear();
            newTaskRequests.clear();

            // Iterate through all jobs that are active on the system and for each running job, get a list of all pending tasks
            // and save this.
            // TODO: Its possible that we may need to use the poll() call with a timeout to delay a bit inside getPendingTasks
            Iterator it = jobsList.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry pair = (Map.Entry)it.next();
                MagellanJob j = (MagellanJob) pair.getValue();
                if(j.getStatus() == MagellanJob.JobState.RUNNING){
                    ArrayList<MagellanTaskRequest> pending = j.getPendingTasks();
                    for(MagellanTaskRequest request : pending){
                        pendingTasksMap.put(request.getId(),request);
                        submittedTaskIdsToJobIds.put(request.getId(),j.getJobID());
                        taskIdsToTaskData.put(request.getId(), request.getData());
                    }
                }
            }
            // Copy all the resource offers into a local datastructure as leasesQueue is accessed by several threads
            leasesQueue.drainTo(newLeases);

            // Pass our list of pending tasks as well as current resource offers to Fenzo and receive a mapping between the two
            SchedulingResult schedulingResult = fenzoScheduler.scheduleOnce(new ArrayList<>(pendingTasksMap.values()), newLeases);
            System.out.println("result=" + schedulingResult);

            // Now use the mesos driver to schedule the tasks
            Map<String,VMAssignmentResult> resultMap = schedulingResult.getResultMap();
            if(!resultMap.isEmpty()) {

                // We now launch tasks on a per host basis. Hosts (VMAssignmentResult) can offer multiple resource offers (called leases)
                for(VMAssignmentResult result: resultMap.values()) {
                    List<Protos.TaskInfo> taskInfos = new ArrayList<>();

                    // Get a list of all the resource offers that will be used for this host
                    List<VirtualMachineLease> leasesUsed = result.getLeasesUsed();
                    StringBuilder stringBuilder = new StringBuilder("Launching on VM " + leasesUsed.get(0).hostname() + " tasks ");
                    final Protos.SlaveID slaveId = leasesUsed.get(0).getOffer().getSlaveId();

                    // For each task that will be run on this host, build a TaskInfo object which will be submitted to
                    // the mesos driver for scheduling
                    for(TaskAssignmentResult t: result.getTasksAssigned()) {
                        stringBuilder.append(t.getTaskId()).append(", ");
                        taskInfos.add(getTaskInfo(slaveId, t.getTaskId()));
                        // remove task from pending tasks map and put into launched tasks map
                        pendingTasksMap.remove(t.getTaskId());
                        launchedTasks.put(t.getTaskId(), leasesUsed.get(0).hostname());
                        // Notify Fenzo that the task is being deployed to a host
                        fenzoScheduler.getTaskAssigner().call(t.getRequest(), leasesUsed.get(0).hostname());
                    }
                    List<Protos.OfferID> offerIDs = new ArrayList<>();
                    // Get a list of all the resource offer ids used for this host.
                    for(VirtualMachineLease l: leasesUsed)
                        offerIDs.add(l.getOffer().getId());

                    System.out.println(stringBuilder.toString());
                    // Finally get the mesos driver to launch the tasks on this host
                    mesosSchedulerDriver.launchTasks(offerIDs, taskInfos);
                }
            }
            // TODO: Posibly remove/increase this?
            try{Thread.sleep(100);}catch(InterruptedException ie){}
        }
    }


    /**
     * Packages the information we want to send over into a TaskInfo construct which we can send
     * Currently commented out as the format of the data send in each task is still unclear
     * @param slaveID
     * @param taskId
     * @return
     */
    private Protos.TaskInfo getTaskInfo(Protos.SlaveID slaveID, final String taskId) {

        Protos.TaskID pTaskId = Protos.TaskID.newBuilder()
                .setValue(taskId).build();
        return Protos.TaskInfo.newBuilder()
                .setName("task " + pTaskId.getValue())
                .setTaskId(pTaskId)
                .setSlaveId(slaveID)
                .addResources(Protos.Resource.newBuilder()
                        .setName("cpus")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(1)))
                .addResources(Protos.Resource.newBuilder()
                        .setName("mem")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(128)))
                .setData((ByteString)taskIdsToTaskData.get(taskId))
                .setExecutor(Protos.ExecutorInfo.newBuilder(getExecutor(taskId)))
                .build();

                // The functions below could be useful for this.
                //.setData(ByteString.copyFromUtf8(data))
                //.setCommand(Protos.CommandInfo.newBuilder().setValue(taskCmdGetter.call(taskId)).build())
                //.setExecutor(ExecutorInfo.newBuilderExecutor))
    }

    /**
     * Given a Bytestring message from an executor, returns the task number
     * @param bs
     * @return
     */
    public String recoverTaskId(ByteString bs){
        JSONObject o = new JSONObject(bs);
        return (String) o.get(MagellanTaskDataJsonTag.UID);
    }

    /**
     * Reuturns state of a job as a jsonobject
     * @param jobID
     * @return
     */
    public JSONObject getJobStatus(Long jobID) {
        MagellanJob mj = jobsList.get(jobID);

        if(mj==null){
            return null;
        }

        JSONObject jsonObj = new JSONObject();
        jsonObj.put("job_id", mj.getJobID());
        jsonObj.put("job_name", mj.getJobName());
        jsonObj.put("job_starting_temp", mj.getJobStartingTemp());
        jsonObj.put("job_cooling_rate", mj.getJobCoolingRate());
        jsonObj.put("job_count", mj.getJobCount());
        jsonObj.put("task_starting_temp", mj.getTaskStartingTemp());
        jsonObj.put("task_cooling_rate", mj.getTaskCoolingRate());
        jsonObj.put("task_count", mj.getTaskCount());
        jsonObj.put("best_location", mj.getBestLocation());
        jsonObj.put("best_energy", mj.getBestEnergy());
        jsonObj.put("energy_history", mj.getEnergyHistory());
        jsonObj.put("num_running_tasks", mj.getNumTasksSent() - mj.getNumFinishedTasks());
        jsonObj.put("num_finished_tasks", mj.getNumFinishedTasks());
        return jsonObj;
    }

    /**
     * Returns the status of all jobs as an array of jsonobjects. Each jsonobject
     * contains the information from getJobStatus()
     * @return
     */
    public ArrayList<JSONObject> getAllJobStatuses() {
        ArrayList<JSONObject> statusAll = new ArrayList<>();

        Iterator it = jobsList.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry pair = (Map.Entry)it.next();
            MagellanJob j = (MagellanJob) pair.getValue();
            statusAll.add(getJobStatus(j.getJobID()));
        }

        return statusAll;
    }

    /**
     * Given a taskID, get a execturo instance
     * @param taskID
     * @return
     */
    private Protos.ExecutorInfo getExecutor(String taskID){
        Long jobID = (Long)submittedTaskIdsToJobIds.get(taskID);
        return ((MagellanJob)jobsList.get(jobID)).getTaskExecutor();
    }

}
