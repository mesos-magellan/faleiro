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

import java.io.UnsupportedEncodingException;
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
            //System.out.println("Task Update: " + taskStatus.getTaskId().getValue() + " in state " + taskStatus.getState());
            switch (taskStatus.getState()) {
                case TASK_ERROR:
                case TASK_FAILED:
                case TASK_LOST:
                    System.out.println("Task Failure. Reason: " + taskStatus.getMessage());
                    break;
                case TASK_FINISHED:
                    // Find which job this task is associated with at forward the message to it
                    try {
                        String data = new String(taskStatus.getData().toByteArray(), "UTF-8");
                        System.out.println("data " + data);
                        System.out.flush();
                        String taskID = recoverTaskId(data);

                        // Process the result of the task by forwarding the data to the job
                        // responsible for its creation
                        processData(data, taskID);

                        // Remove the tasks from data structures
                        submittedTaskIdsToJobIds.remove(taskID);

                        //Notify Fenzo that the task has completed and is no longer assigned
                        fenzoScheduler.getTaskUnAssigner().call(taskStatus.getTaskId().getValue(), launchedTasks.get(taskStatus.getTaskId().getValue()));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
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

    private TaskScheduler fenzoScheduler;
    private MesosSchedulerDriver mesosSchedulerDriver;
    private boolean initialized = false;
    private  long numCreatedJobs = 0;
    private final AtomicBoolean frameworkHasShutdown = new AtomicBoolean(true);
    private final AtomicReference<MesosSchedulerDriver> mesosDriver = new AtomicReference<>();
    private final ConcurrentHashMap<Long, MagellanJob> jobsList = new ConcurrentHashMap<>();
    private final BlockingQueue<VirtualMachineLease> leasesQueue = new LinkedBlockingQueue<>();
    private final Map<String, MagellanTaskRequest> pendingTasksMap = new HashMap<>();
    private final HashMap<String, Long> submittedTaskIdsToJobIds = new HashMap<>();
    private final HashMap<String, String> launchedTasks = new HashMap<>();

    public MagellanFramework(){
        fenzoScheduler = new TaskScheduler.Builder()
                .withLeaseOfferExpirySecs(1000000000)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    public void call(VirtualMachineLease lease) {
                        System.out.println("Declining offer on " + lease.hostname());
                        mesosDriver.get().declineOffer(lease.getOffer().getId());
                    }
                })
                .build();
    }

    /**
     * Shutsdown the framework. The shutdown happens during or shortly after this call
     * returns
     * @return status of shutdown
     */
    public Protos.Status shutdownFramework() {
        System.out.println("Shutting down mesos driver");
        Protos.Status status = mesosSchedulerDriver.stop();
        frameworkHasShutdown.set(true);
        initialized = false;
        return status;
    }

    /**
     * Initializes the framework by creating necessary data structures to connect to the master.
     * This method only initializes the framework. To start and connect the framework to the master,
     * This method returns without doing anything if this method is called while the framework is running.
     * call startFramework()
     * @param mesosMasterIP     - IP Address of the mesos master
     */
    public void initializeFramework(String mesosMasterIP){
        // Dont initialize while running
        if(!frameworkHasShutdown.get()){
            return;
        }

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
        mesosDriver.set(mesosSchedulerDriver);
        frameworkHasShutdown.set(false);
        initialized = true;
    }


    /**
     * Starts the mesos driver in another thread which connects to the master.
     * Starts the main framework loop in another thread
     *
     * Call to this method is ignored if the framework is not initialized by a previous
     * call to initializeFramework() or if the framework is already running.
     */
    public void startFramework(){
        if(!initialized){
            System.err.println("Initialize the framework first using MagellanFramework::initialize() before calling this method");
            return;
        }

        if(!frameworkHasShutdown.get()) {
            System.err.println("Framework is already running");
        }

        // Start the driver
        new Thread() {
            public void run() {
                mesosSchedulerDriver.run();
            }
        }.start();
        mesosSchedulerDriver.stop();

        // Start the framework
        new Thread(() -> {
            runFramework();
        }).start();

    }

    /**
     * Creates a job and runs it on a separate thread
     *
     * @param jobName Name of job
     * @param jobStartingTemp Starting temperature of job. Higher means job runs for longer
     * @param jobCoolingRate Rate at which temperature depreciates each time
     * @param jobIterationsPerTemp number of iterations per temperature for job
     * @param taskName - Name of the task on the executor to run
     * @param taskTime - How long to run each task for.
     * @param additionalParameters Additional job parameters
     *
     * @return An ID number greater or equal to 0 if successful
     *          -1 if invalid parameters
     */
    public long createJob(String jobName,
                          int jobStartingTemp,
                          double jobCoolingRate,
                          int jobIterationsPerTemp,
                          int taskTime,
                          String taskName,
                          JSONObject additionalParameters)
    {

        if (jobName == null ||
                jobStartingTemp <= 0 ||
                jobCoolingRate <= 0 ||
                jobIterationsPerTemp <= 0 ||
                taskTime <= 0 ||
                taskName == null ||
                additionalParameters == null)
        {
            // One or more of the parameters have invalid values
            return -1;
        }


        long id = numCreatedJobs++;
        MagellanJob j = new MagellanJob(id,
                                        jobName,
                                        jobStartingTemp,
                                        jobCoolingRate,
                                        jobIterationsPerTemp,
                                        taskTime,
                                        taskName,
                                        additionalParameters);
        jobsList.put(id, j);

        j.start();

        return id;
    }

    /**
     *  This contains the main loop of the program. In here, the framework queries
     *  each running job in the system to get a list of tasks each job wants to run.
     *  These set of tasks are then given to Fenzo which matches available resource
     *  offers from Mesos to tasks.The resulting matches from Fenzo are then given
     *  to the Mesos Driver for execution.
     */
    private void runFramework(){
        System.out.println("Running all");
        List<VirtualMachineLease> newLeases = new ArrayList<>();
        List<TaskRequest> newTaskRequests = new ArrayList<>();

        while(true) {
            // Only if the framework has shutdown do we exist our main loop
            if(frameworkHasShutdown.get()) {
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
                if(j.getState() == MagellanJob.JobState.RUNNING){
                    ArrayList<MagellanTaskRequest> pending = j.getPendingTasks();
                    for(MagellanTaskRequest request : pending){
                        pendingTasksMap.put(request.getId(),request);
                        submittedTaskIdsToJobIds.put(request.getId(),j.getJobID());
                        //taskIdsToTaskData.put(request.getId(), request.getData());
                    }
                }
            }
            // Copy all the resource offers into a local datastructure as leasesQueue is accessed by several threads
            leasesQueue.drainTo(newLeases);

            // Pass our list of pending tasks as well as current resource offers to Fenzo and receive a mapping between the two
            SchedulingResult schedulingResult = fenzoScheduler.scheduleOnce(new ArrayList<>(pendingTasksMap.values()), newLeases);
            //System.out.println("result=" + schedulingResult);

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
     * Stops a job from submitting new tasks to the framework.
     * Statistics on the jobs progress at the time this method is called are
     * available by calling getJobStatus(). Tasks that have already been sent out
     * are still processed when the tasks come back from the executors and the
     * statistics are updated accordingly
     *
     * @param jobID     ID of the job to stop
     */
    public void stopJob(Long jobID) {
        MagellanJob j = jobsList.get(jobID);
        if(j!=null){
            j.stop();
        }
    }

    /**
     * Pauses a job. This means new tasks are not submitted the framework.
     * Tasks that have already been sent out are still processed and the job
     * statistics are updated accordingly. To resume the job, call resumeJob()
     * Calling this method on a job that has terminated naturally or by a call
     * to stopJob() is ignored
     * @param jobID     ID of the job to pause
     */
    public void pauseJob(Long jobID) {
        MagellanJob j = jobsList.get(jobID);
        if(j!=null){
            j.pause();
        }
    }

    /**
     * Resumes a paused job. This means that the job can continue passing new
     * tasks to the framework.
     * Calling this method on a job that has terminated naturally or by a call
     * to stopJob() is ignored
     *
     * @param jobID     ID of job to resume
     */
    public void resumeJob(Long jobID){
        MagellanJob j = jobsList.get(jobID);
        if(j!=null){
            j.resume();
        }
    }


    /**
     * Packages the information we want to send over into a TaskInfo construct which we can send
     * @param slaveID   - ID of slave where this task will run
     * @param taskId    - Used to retrieve task specific data and the executor used to run
 *                        the task
     * @return
     */
    private Protos.TaskInfo getTaskInfo(Protos.SlaveID slaveID, final String taskId) {

        Protos.TaskID pTaskId = Protos.TaskID.newBuilder().setValue(taskId).build();

        // Create a TaskInfo object that encapsulates all the necessary information for a task
        // to reach its destination and run successfully on the executor.
        return Protos.TaskInfo.newBuilder()
                .setName("task " + pTaskId.getValue())
                .setTaskId(pTaskId)
                .setSlaveId(slaveID)
                .addResources(Protos.Resource.newBuilder()
                        .setName("cpus")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(getCpus(taskId))))
                .addResources(Protos.Resource.newBuilder()
                        .setName("mem")
                        .setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(getMem(taskId))))
                .setData(getData(taskId))
                .setExecutor(Protos.ExecutorInfo.newBuilder(getExecutor(taskId)))
                .build();

                // The functions below could be useful for this.
                //.setData(ByteString.copyFromUtf8(data))
                //.setCommand(Protos.CommandInfo.newBuilder().setValue(taskCmdGetter.call(taskId)).build())
                //.setExecutor(ExecutorInfo.newBuilderExecutor))
    }

    /**
     * Returns the number of cpu's requested by a task
     * @param taskId
     * @return
     */
    private double getCpus(String taskId){
        return pendingTasksMap.get(taskId).getCPUs();
    }

    /**
     * Returns the amount of memory requested by a task
     * @param taskId
     * @return
     */
    private double getMem(String taskId){
        return pendingTasksMap.get(taskId).getMemory();
    }

    /**
     * Returns the task specific data/parameters given to it by the job
     * responsible for its creation.
     * @param taskId
     * @return
     */
    private ByteString getData(String taskId){
        return pendingTasksMap.get(taskId).getData();
    }

    /**
     * Given a String message in UTF-8 from an executor, returns the task number
     * @param data
     * @return
     */
    private String recoverTaskId(String data){
        JSONObject o = new JSONObject(data);
        return (String) o.get(MagellanTaskDataJsonTag.UID);
    }

    /**
     * Returns the state of a job as a JSONObject. This can be sent back to the
     * customer or used by zookeeper to persist the state of the framework and
     * jobs in the system
     * @param jobID
     * @return JSONObject where the key is attributes that store the state
     *          of each job
     */
    public JSONObject getJobStatus(Long jobID) {
        MagellanJob mj = jobsList.get(jobID);

        if(mj==null){
            return null;
        }

        return mj.getClientFriendlyStatus();
    }

    /**
     * Returns true if the job is either done or stopped or if the Job DNE.
     * Returns false if the job is paused or running
     * @param jobID
     * @return
     */
    public boolean isDone(Long jobID){
        MagellanJob mj = jobsList.get(jobID);

        if(mj==null){
            return true;
        }

        return mj.isDone();
    }

    /**
     * Returns the status of all jobs as an array of JSONObject. Each JSONObject
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
     * Given a taskID, get the executor instance the task will run on
     * @param taskID
     * @return
     */
    private Protos.ExecutorInfo getExecutor(String taskID){
        Long jobID = submittedTaskIdsToJobIds.get(taskID);
        return jobsList.get(jobID).getTaskExecutor();
    }

    /**
     * This method is used to pass the result of a finished task to the
     * job that created the task
     * @param taskResult    - Response of finished task that needs to be processed
     * @param taskID        - Task Id of task that just finished
     */
    private void processData(String taskResult, String taskID) {
        long jobId = submittedTaskIdsToJobIds.get(taskID);
        jobsList.get(jobId).processIncomingMessages(taskResult);
    }

}
