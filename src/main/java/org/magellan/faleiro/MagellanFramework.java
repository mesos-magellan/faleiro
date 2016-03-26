package org.magellan.faleiro;

import com.google.protobuf.ByteString;
import com.netflix.fenzo.*;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.VMLeaseObject;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.magellan.faleiro.JsonTags.TaskData;
import static org.magellan.faleiro.JsonTags.VerboseStatus;
import static org.magellan.faleiro.JsonTags.SimpleStatus;

public class MagellanFramework implements Watcher {

    private static final Logger log = Logger.getLogger(MagellanFramework.class.getName());

    class MagellanScheduler implements Scheduler {

        public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
            log.log(Level.FINE, "Registered! ID = " + frameworkID.getValue());
            fenzoScheduler.expireAllLeases();
        }

        public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
            log.log(Level.FINE, "Re-registered " + masterInfo.getId());
            fenzoScheduler.expireAllLeases();
        }

        public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {
            for(Protos.Offer offer: offers) {
                log.log(Level.FINE, "Adding offer " + offer.getId() + " from host " + offer.getHostname());
                leasesQueue.offer(new VMLeaseObject(offer));
            }
        }

        public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {
            fenzoScheduler.expireLease(offerID.getValue());
        }

        public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
            switch (taskStatus.getState()) {
                case TASK_ERROR:
                case TASK_FAILED:
                case TASK_LOST:
                    log.log(Level.WARNING, "Task Failure. Reason: " + taskStatus.getMessage());
                    break;
                case TASK_FINISHED:
                    // Find which job this task is associated with at forward the message to it
                    try {
                        String data = new String(taskStatus.getData().toByteArray(), "UTF-8");
                        String taskID = recoverTaskId(data);

                        // Process the result of the task by forwarding the data to the job
                        // responsible for its creation
                        processData(data, taskID);

                        // Remove the tasks from data structures
                        submittedTaskIdsToJobIds.remove(taskID);

                        //Notify Fenzo that the task has completed and is no longer assigned
                        fenzoScheduler.getTaskUnAssigner().call(taskStatus.getTaskId().getValue(), launchedTasks.get(taskStatus.getTaskId().getValue()));
                    } catch (UnsupportedEncodingException e) {
                        log.log(Level.SEVERE, e.getMessage());
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
    private DataMonitor dataMonitor = null;
    private  long numCreatedJobs = 0;
    private final AtomicReference<MesosSchedulerDriver> mesosDriver = new AtomicReference<>();
    private final ConcurrentHashMap<Long, MagellanJob> jobsList = new ConcurrentHashMap<>();
    private final BlockingQueue<VirtualMachineLease> leasesQueue = new LinkedBlockingQueue<>();
    private final Map<String, MagellanTaskRequest> pendingTasksMap = new HashMap<>();
    private final HashMap<String, Long> submittedTaskIdsToJobIds = new HashMap<>();
    private final HashMap<String, String> launchedTasks = new HashMap<>();
    private Watcher zookeeperWatcher = null;
    private ZookeeperService zk = null;

    public MagellanFramework(){
        fenzoScheduler = new TaskScheduler.Builder()
                .withLeaseOfferExpirySecs(1000000000)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    public void call(VirtualMachineLease lease) {
                        log.log(Level.INFO, "Declining offer on " + lease.hostname());
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
        log.log(Level.INFO, "Shutting down mesos driver");
        Protos.Status status = mesosSchedulerDriver.stop();
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
        // Connect to zookeeper
        try {
            String zAddr = System.getenv("ZK_IP") + ":" + System.getenv("ZK_PORT");
            zk = new ZookeeperService(zAddr,this);
        } catch (IOException e) {
            log.log(Level.SEVERE, e.getMessage());
        }

        // Pause for a short amount of time to let other to let other schedulers start
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            log.log(Level.SEVERE, e.getMessage());
        }


        // Undergo leader election and block until current scheduler is leader
        LeaderElection leader = new LeaderElection(zk);
        zookeeperWatcher = leader;
        leader.initialize();
        leader.blockUntilElectedLeader();

        Scheduler mesosScheduler = new MagellanScheduler();

        Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder()
                .setUser(System.getenv("FRAMEWORK_USER"))
                .setName("Simulated Annealing Scheduler")
                .setPrincipal(System.getenv("PRINCIPAL"));


        if (System.getenv("MESOS_AUTHENTICATE") != null) {
            log.log(Level.CONFIG, "Enabling authentication for the framework");

            if (System.getenv("PRINCIPAL") == null) {
                log.log(Level.SEVERE, "Expecting authentication principal in the environment");
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

        // Create a datamonitor which will be used to perisist the scheduler's state
        dataMonitor  = new DataMonitor(zk, System.getenv("ZKNODE_PATH"), this);
        zookeeperWatcher = dataMonitor;
        dataMonitor.initialize();

        //Retrieve previous state of scheduler if it exists and intialize the scheduler
        //with this
        JSONObject pstate = dataMonitor.getInitialState();
        if(pstate != null && pstate.length()>0){
            log.log(Level.INFO, "Restoring previous framework state from Zookeeper");
            restorePreviousState(pstate);
        }else{
            log.log(Level.INFO, "No past state found on Zookeeper. Starting new framework");
        }

        mesosDriver.set(mesosSchedulerDriver);
    }

    /**
     * If a previous state for the scheduler exists, restore it
     * @param jso : JSonObject from Zookeeper that contains all the necessary information about a job.
     */
    private void restorePreviousState(JSONObject jso){
        numCreatedJobs = jso.getInt("num_created_jobs");

        JSONArray jobs = jso.getJSONArray("jobs");
        for(int i = 0; i < jobs.length(); i++){
            JSONObject jsonobject = jobs.getJSONObject(i);
            jobsList.put(jsonobject.getLong("job_id"),new MagellanJob(jsonobject));
        }

    }


    /**
     * Callback function for zookeeper events. MagellanFramework itself wont handle the
     * events so forward the event to the current watcher. Only one watcher can be
     * active at a time.
     * @param watchedEvent
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        if(zookeeperWatcher!=null) {
            zookeeperWatcher.process(watchedEvent);
        }
    }


    /**
     * Starts the mesos driver in another thread which connects to the master.
     * Starts the main framework loop in another thread
     *
     * Call to this method is ignored if the framework is not initialized by a previous
     * call to initializeFramework() or if the framework is already running.
     */

    public void startFramework(){
        /*if(!initialized){
            System.err.println("Initialize the framework first using MagellanFramework::initialize() before calling this method");
            return;
        }

        if(!frameworkHasShutdown.get()) {
            System.err.println("Framework is already running");
        }*/

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

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            log.log(Level.SEVERE, e.getMessage());
        }

        // Start any jobs that we restored from Zookeeper
        Iterator it = jobsList.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry pair = (Map.Entry)it.next();
            MagellanJob j = (MagellanJob) pair.getValue();
            if(j.getState() == MagellanJob.JobState.RUNNING || j.getState() == MagellanJob.JobState.INITIALIZED)
            {
                j.start();
            }
        }

    }

    /**
     * Creates a job and runs it on a separate thread
     *
     * @param jobName Name of job
     * @param taskName - Name of the task on the executor to run
     * @param taskTime - How long to run each task for.
     * @param additionalParameters Additional job parameters
     *
     * @return An ID number greater or equal to 0 if successful
     *          -1 if invalid parameters
     */
    public long createJob(String jobName,
                          int taskTime,
                          String taskName,
                          JSONObject additionalParameters)
    {

        if (jobName == null ||
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
        log.log(Level.INFO, "Running Framework");
        List<VirtualMachineLease> newLeases = new ArrayList<>();
        List<TaskRequest> newTaskRequests = new ArrayList<>();

        while(true) {

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

                    log.log(Level.INFO, stringBuilder.toString());
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
        return (String) o.get(TaskData.UID);
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

    /**
     * Returns the status of a job as a JSONObject.
     * @param jobID
     * @return JSONObject where the key is attributes that store the state
     *          of each job
     */
    public JSONObject getSimpleJobStatus(Long jobID) {
        MagellanJob mj = jobsList.get(jobID);

        if(mj==null){
            return null;
        }

        return mj.getSimpleStatus();
    }

    /**
     * Returns entire state/contents of framework as a JSONObject. Used to persist in zookeeper.
     * Not for client
     * @return
     */
    public JSONObject getVerboseSystemInfo(){
        JSONObject sysState = new JSONObject();
        sysState.put("num_created_jobs", numCreatedJobs);
        sysState.put("jobs",getVerboseAllJobInfo());
        return sysState;
    }


    /**
     * Returns the status of all jobs as an JSONArray. Only returns information pertaining to
     * problem being sovled. Internal state of Job is not returned
     * @return
     */
    public JSONArray getSimpleAllJobStatuses() {
        JSONArray statusAll = new JSONArray();

        Iterator it = jobsList.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry pair = (Map.Entry)it.next();
            MagellanJob j = (MagellanJob) pair.getValue();
            statusAll.put(j.getSimpleStatus());
        }

        return statusAll;
    }

    /**
     * Returns the status of all jobs as an JSONArray. The information returned for each
     * job is verbose and is only intended to be used to save state in zookeeper. For a client
     * friendly version without internal state, use getAllJobStatuses()
     * @return
     */
    private JSONArray getVerboseAllJobInfo() {
        JSONArray statusAll = new JSONArray();

        Iterator it = jobsList.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry pair = (Map.Entry)it.next();
            MagellanJob j = (MagellanJob) pair.getValue();
            statusAll.put(j.getStateSnapshot());
        }

        return statusAll;
    }

}
