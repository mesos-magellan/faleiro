package org.magellan.faleiro;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.ByteString;
import org.apache.mesos.Protos;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.BitSet;

import static org.magellan.faleiro.JsonTags.TaskData;
import static org.magellan.faleiro.JsonTags.VerboseStatus;
import static org.magellan.faleiro.JsonTags.SimpleStatus;

public class MagellanJob {
    private static final Logger log = Logger.getLogger(MagellanJob.class.getName());

    // These constants are used to tell the framework how much of each
    // resource each task created by this job needs to execute
    private final double NUM_CPU;
    private final double NUM_MEM;
    private final double NUM_NET_MBPS;
    private final double NUM_DISK;
    private final int NUM_PORTS;

    private final long jobID;

    private final String jobName;

    private final long jobStartingTime;

    // How long each task runs for
    private int jobTaskTime;

    // The current best solution as determined by all the running tasks
    private String jobCurrentBestSolution = "";

    // The energy of the current best solution. In our system, a lower energy translates to a better solution
    private double jobBestEnergy = Double.MAX_VALUE;

    // This comes from the client and tells the agent the name of the executor to run for tasks created by this job
    private String jobTaskName;

    // Additional parameters passed in from the user
    private JSONObject jobAdditionalParam = null;

    // A list of the best energies found by every task run by this job.
    //private ConcurrentLinkedDeque<Double> energyHistory = new ConcurrentLinkedDeque<>();
    private JSONArray energyHistory = new JSONArray();

    // This list stores tasks that are ready to be scheduled. This list is then consumed by the
    // MagellanFramework when it is ready to accept new tasks.
    private BlockingQueue<MagellanTaskRequest> pendingTasks = new LinkedBlockingQueue<>();

    private JobState state = JobState.INITIALIZED;

    private Protos.ExecutorInfo taskExecutor;

    /* lock to wait for division task to complete */
    private Object division_lock = new Object();

    private Boolean division_is_done = false;

    /* task ID of division, waiting until this is returned to make more tasks */
    private String divisionTaskId;

    /* json array of returned division. iterated through to make new tasks */
    private JSONArray returnedResult;

    private BitSet finishedTasks = null;

    private int currentTask;

    /**
     *
     * @param id Unique Job id
     * @param jName Name of job
     * @param taskName Name of the task we want to execute on the executor side
     * @param jso Additional Job param
     */
    public MagellanJob(long id,
                       String jName,
                       int taskTime,
                       String taskName,
                       JSONObject jso)
    {
        jobID = id;
        jobName = jName;
        jobTaskTime = taskTime;
        jobTaskName = taskName;
        jobAdditionalParam = jso;
        taskExecutor = registerExecutor(System.getenv("EXECUTOR_PATH"));
        jobStartingTime = System.currentTimeMillis();

        NUM_CPU = 1;
        NUM_MEM = 32;
        NUM_NET_MBPS = 0;
        NUM_DISK = 0;
        NUM_PORTS = 0;

        log.log(Level.CONFIG, "New Job created. ID is " + jobID);
    }

    /**
     *
     * @param j : JSONObject from zookeeper used for creating a new job on this framework based on
     *            the state of a job from another, deceased framework
     */
    public MagellanJob(JSONObject j){

        //Reload constants
        NUM_CPU = j.getDouble(VerboseStatus.NUM_CPU);
        NUM_MEM = j.getDouble(VerboseStatus.NUM_MEM);
        NUM_NET_MBPS = j.getDouble(VerboseStatus.NUM_NET_MBPS);
        NUM_DISK = j.getDouble(VerboseStatus.NUM_DISK);
        NUM_PORTS = j.getInt(VerboseStatus.NUM_PORTS);
        if(j.has(VerboseStatus.BITFIELD_FINISHED)) {
            finishedTasks = Bits.convert(j.getLong(VerboseStatus.BITFIELD_FINISHED));
            division_is_done = j.getBoolean(VerboseStatus.DIVISION_IS_FINISHED);
            returnedResult = j.getJSONArray(TaskData.RESPONSE_DIVISIONS);
        }

        jobID = j.getInt(SimpleStatus.JOB_ID);
        jobStartingTime = j.getLong(SimpleStatus.JOB_STARTING_TIME);
        jobName = j.getString(SimpleStatus.JOB_NAME);
        jobTaskTime = j.getInt(SimpleStatus.TASK_SECONDS);
        jobTaskName = j.getString(SimpleStatus.TASK_NAME);
        jobCurrentBestSolution = j.getString(SimpleStatus.BEST_LOCATION);
        jobBestEnergy = j.getDouble(SimpleStatus.BEST_ENERGY);
        energyHistory = j.getJSONArray(SimpleStatus.ENERGY_HISTORY);
        jobAdditionalParam = j.getJSONObject(SimpleStatus.ADDITIONAL_PARAMS);
        state = (new Gson()).fromJson(j.getString(SimpleStatus.CURRENT_STATE), JobState.class);


        taskExecutor = registerExecutor("/usr/local/bin/enrique");

        log.log(Level.CONFIG, "Reviving job from zookeeper. State is " + state + " . Id is " + jobID);
    }

    /**
     *  Creates an executor object. This object will eventually be run on an agent when it get schedules
     * @param pathToExecutor
     * @return
     */
    public Protos.ExecutorInfo registerExecutor(String pathToExecutor){
        return  Protos.ExecutorInfo.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue("default"))
                .setCommand(Protos.CommandInfo.newBuilder().setValue(pathToExecutor))
                .setName("SA Job Executor")
                .setSource("java_test")
                .build();
    }

    /**
     * Runs the main loop in a separate thread
     */
    public void start() {
        state = JobState.RUNNING;

        new Thread(() -> {
            run();
        }).start();
    }

    /**
     * This functions creates tasks that are passed to the magellan framework using an annealing approach.
     * We determine the starting location of each task using a temperature cooling mechanism where early on
     * in the execution of this job, more risks are taken and more tasks run in random locations in an attempt
     * to explore more of the search space. As time increases and the temperature of the job decreases, tasks
     * are given starting locations much closer to the global, best solution for this job so that the neighbors
     * of the best solution are evaluated thoroughly in the hopes that they lie close to the global maximum.
     */
    private void run() {
        /* first create the splitter task */
        if(state == JobState.STOP) {
            return;
        }

        while(state==JobState.PAUSED){
            Thread.yield();
            // wait while job is paused
        }

        try {
            // To keep the task ids unique throughout the global job space, use the job ID to
            // ensure uniqueness
            String newTaskId = "" + jobID + "_" + "div";

            // Choose the magellan specific parameters for the new task
            //ByteString data = pickNewTaskStartingLocation(jobTaskTime, jobTaskName, newTaskId, jobAdditionalParam);

            int divisions = 0;

            // Add the task to the pending queue until the framework requests it
            MagellanTaskRequest newTask = new MagellanTaskRequest(
                    newTaskId,
                    jobName,
                    NUM_CPU,
                    NUM_MEM,
                    NUM_NET_MBPS,
                    NUM_DISK,
                    NUM_PORTS,
                    packTaskData(newTaskId, jobTaskName, TaskData.RESPONSE_DIVISIONS, jobAdditionalParam, divisions)
            );
            
            pendingTasks.put(newTask);
            divisionTaskId = newTaskId;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        /* lock until notified that division has returned */
        log.log(Level.INFO, "waiting for to enter division_lock sync block");
        synchronized (division_lock) {
            try {
                log.log(Level.INFO, "waiting for division_is_done");
                while (division_is_done.booleanValue() == false) {
                    division_lock.wait();
                }
            } catch (InterruptedException e) {
                log.log(Level.SEVERE, e.getMessage());
            }
        }

        /* got result of division task */
        finishedTasks = new BitSet(returnedResult.length()); // initialize list of isFinished bits for each task. Persisted across crash.

        for (currentTask = 0; currentTask < returnedResult.length(); currentTask++) {

            while(state==JobState.PAUSED){
                Thread.yield();
                // wait while job is paused
            }

            if(state == JobState.STOP) {
                return;
            }

            // check if this index was already completed in a previous run, if so skip it
            if(!finishedTasks.get(currentTask)){
                 /* got a list of all the partitions, create a task for each */
                try {
                    String newTaskId = "" + jobID + "_" + currentTask;

                    MagellanTaskRequest newTask = new MagellanTaskRequest(
                            newTaskId,
                            jobName,
                            NUM_CPU,
                            NUM_MEM,
                            NUM_NET_MBPS,
                            NUM_DISK,
                            NUM_PORTS,
                            packTaskData(newTaskId, jobTaskName, "anneal", jobTaskTime/(60.0 * returnedResult.length()), jobAdditionalParam, returnedResult.get(currentTask))
                    );

                    // Add the task to the pending queue until the framework requests it
                    pendingTasks.put(newTask);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        log.log(Level.INFO, "Finished sending tasks. Waiting now. Tasks sent = " + returnedResult.length());

        while(state != JobState.STOP && (returnedResult.length() != finishedTasks.cardinality())) {
            // wait for all tasks to finish
            Thread.yield();
        }

        if(state!=JobState.STOP) {
            state = JobState.DONE;
        }
        log.log(Level.INFO, "[Job " + jobID + "]" + " done. Best fitness (" + jobBestEnergy + ") achieved at location " + jobCurrentBestSolution);
    }

    /**
     * Called by the magellan framework to get a list of tasks that this job wants scheduled.
     * @return
     */
    public ArrayList<MagellanTaskRequest> getPendingTasks(){
        ArrayList<MagellanTaskRequest> pt = new ArrayList<>();
        pendingTasks.drainTo(pt);
        return pt;
    }

    /**
     * Job is done if it terminates naturally or if it receives an explicit signal to stop
     * @return
     */
    public boolean isDone(){
        return state == JobState.DONE || state == JobState.STOP;
    }

    /**
     * Called by magellan framework when a message from the executor is sent to this job. This message
     * could indicate that the task was successful, or failed.
     * @param taskState : Indicates the status of the task. Could be TASK_FINISHED, TASK_ERROR, TASK_FAILED,
     *                  TASK_LOST
     * @param taskId : Id of the task
     * @param data   : Data of the task
     */
    public void processIncomingMessages(Protos.TaskState taskState, String taskId, String data) {
        log.log(Level.INFO, "processIncomingMessages: state: " + state + " , taskId: " + taskId);
        log.log(Level.FINER, "data: " + data);
        boolean isDiv = true;
        String[] parts = taskId.split("_");
        String strReturnedJobId = parts[0];
        long returnedJobId = Integer.parseInt(strReturnedJobId);
        int returnedTaskNum = 0;
        String strReturnedTaskNum = parts[1];

        // check that task result is for me, should always be true
        if(returnedJobId != this.jobID){
            log.log(Level.SEVERE, "Job: " + getJobID() + " got task result meant for Job: " + returnedJobId);
            System.exit(-1);
        }

        if(!parts[1].equals("div")) {
            isDiv = false;
            returnedTaskNum = Integer.parseInt(strReturnedTaskNum);
        }

        switch (taskState) {
            case TASK_ERROR:
            case TASK_FAILED:
            case TASK_LOST:
                if(state != JobState.STOP){
                    log.log(Level.WARNING, "Problem with task, rescheduling it");

                    try {
                        String newTaskId = taskId;
                        MagellanTaskRequest newTask;
                        if(isDiv){
                            int divisions = 0;

                            // Add the task to the pending queue until the framework requests it
                            newTask = new MagellanTaskRequest(
                                    newTaskId,
                                    jobName,
                                    NUM_CPU,
                                    NUM_MEM,
                                    NUM_NET_MBPS,
                                    NUM_DISK,
                                    NUM_PORTS,
                                    packTaskData(newTaskId, jobTaskName, TaskData.RESPONSE_DIVISIONS, jobAdditionalParam, divisions)
                            );
                        }else {
                            newTask = new MagellanTaskRequest(
                                    newTaskId,
                                    jobName,
                                    NUM_CPU,
                                    NUM_MEM,
                                    NUM_NET_MBPS,
                                    NUM_DISK,
                                    NUM_PORTS,
                                    packTaskData(newTaskId, jobTaskName, "anneal", jobTaskTime / (60.0 * returnedResult.length()), jobAdditionalParam, returnedResult.get(currentTask))
                            );
                        }
                        while(state==JobState.PAUSED){
                            Thread.yield();
                            // wait while job is paused
                        }
                        // Add the task to the pending queue until the framework requests it
                        pendingTasks.put(newTask);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                else{
                    // ignore the response, task was killed intentionally
                    return;
                }
        }

        if(data == null){
            return;
        }

        // Retrieve the data sent by the executor
        JSONObject js = new JSONObject(data);

        String returnedTaskId = js.getString(TaskData.UID);

        log.log(Level.INFO, "returnedTaskId is " + returnedTaskId);
        log.log(Level.INFO, "divisionTaskId is " + divisionTaskId);

        if(returnedTaskId.equals(divisionTaskId)) {
            log.log(Level.INFO, "equal, now waiting for division_lock");
            synchronized (division_lock) {
                log.log(Level.INFO, "equal, got division_lock");
                /* parse out the result to get list of tasks */
                returnedResult = js.getJSONArray(TaskData.RESPONSE_DIVISIONS);
                division_is_done = true;
                log.log(Level.INFO, "notifying division_lock");
                division_lock.notify();
            }
            return;
        }
        /* not an error and not a division, get results */
        double fitness_score = js.getDouble(TaskData.FITNESS_SCORE);
        String best_location = js.getString(TaskData.BEST_LOCATION);

        finishedTasks.set(returnedTaskNum); // mark task as finished. needed for zookeeper state revival

        energyHistory.put(fitness_score);
        // If a better score was discovered, make this our global, best location
        if(fitness_score < jobBestEnergy) {
            jobCurrentBestSolution = best_location;
            jobBestEnergy = fitness_score;
        }
        log.log(Level.FINE, "Job: " + getJobID() + " processed finished task");
    }

    public void stop() {
        log.log(Level.INFO, "Job: " + getJobID() + " asked to stop");
        state = JobState.STOP;
    }

    public void pause() {
        if(!isDone()) {
            log.log(Level.INFO, "Job: " + getJobID() + " asked to pause");
            state = JobState.PAUSED;
        }
    }

    public void resume(){
        if(!isDone()) {
            log.log(Level.INFO, "Job: " + getJobID() + " asked to resume");
            state = JobState.RUNNING;
        }
    }

    /**
     * Called by the zookeeper service to transfer a snapshot of the current state of the job to save in
     * case this node goes down. This contains information from getSimpleStatus() as well as
     * additional, internal information
     *
     * @return A snapshot of all the important information in this job
     */
    public JSONObject getStateSnapshot() {
        JSONObject jsonObj = getSimpleStatus();

        // Store constants
        jsonObj.put(VerboseStatus.NUM_CPU, NUM_CPU);
        jsonObj.put(VerboseStatus.NUM_MEM, NUM_MEM);
        jsonObj.put(VerboseStatus.NUM_NET_MBPS, NUM_NET_MBPS);
        jsonObj.put(VerboseStatus.NUM_DISK, NUM_DISK);
        jsonObj.put(VerboseStatus.NUM_PORTS, NUM_PORTS);

        if(division_is_done.booleanValue() == false){
            // if null wipe entry
            jsonObj.put(VerboseStatus.DIVISION_IS_FINISHED, false);
        }else {
            /* save all three states after division is complete */
            jsonObj.put(VerboseStatus.BITFIELD_FINISHED, Bits.convert(finishedTasks));
            jsonObj.put(TaskData.RESPONSE_DIVISIONS, returnedResult);
            jsonObj.put(VerboseStatus.DIVISION_IS_FINISHED, division_is_done);
        }

        return  jsonObj;
    }

    /**
     * This method returns information that the client wants to know about the job
     * @return
     */
    public JSONObject getSimpleStatus() {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put(SimpleStatus.JOB_ID, getJobID());
        jsonObj.put(SimpleStatus.JOB_NAME, getJobName());
        jsonObj.put(SimpleStatus.JOB_STARTING_TIME, getStartingTime());
        jsonObj.put(SimpleStatus.TASK_SECONDS, getTaskTime());
        jsonObj.put(SimpleStatus.TASK_NAME, getJobTaskName());
        jsonObj.put(SimpleStatus.BEST_LOCATION, getBestLocation());
        jsonObj.put(SimpleStatus.BEST_ENERGY, getBestEnergy());
        jsonObj.put(SimpleStatus.ENERGY_HISTORY, getEnergyHistory());
        jsonObj.put(SimpleStatus.NUM_FINISHED_TASKS, getNumFinishedTasks());
        jsonObj.put(SimpleStatus.NUM_TOTAL_TASKS, getNumTotalTasks());
        jsonObj.put(SimpleStatus.ADDITIONAL_PARAMS, getJobAdditionalParam());
        jsonObj.put(SimpleStatus.CURRENT_STATE, getState());
        return jsonObj;
    }

    /**
     * Takes the given parameters and packages it into a json formatted Bytestring which can be
     * packaged into a TaskInfo object by the magellan framework
     * @param newTaskId
     * @param jobTaskName
     * @param command
     * @param jobAdditionalParam
     * @param taskData
     * @return
     */
    private ByteString packTaskData(String newTaskId, String jobTaskName, String command, double taskTime, JSONObject jobAdditionalParam, Object taskData){
        JSONObject jsonTaskData = new JSONObject();
        jsonTaskData.put(TaskData.MINUTES_PER_DIVISION, taskTime);
        jsonTaskData.put(TaskData.TASK_DATA, taskData);
        return packTaskData(jsonTaskData, newTaskId, jobTaskName, command, jobAdditionalParam);
    }

    private ByteString packTaskData(String newTaskId, String jobTaskName, String command, JSONObject jobAdditionalParam, int divisions){
        JSONObject jsonTaskData = new JSONObject();
        jsonTaskData.put(TaskData.TASK_DIVISIONS, divisions);
        return packTaskData(jsonTaskData, newTaskId, jobTaskName, command, jobAdditionalParam);
    }

    private ByteString packTaskData(JSONObject jsonTaskData, String newTaskId, String jobTaskName, String command, JSONObject jobAdditionalParam){
        jsonTaskData.put(TaskData.UID, newTaskId);
        jsonTaskData.put(TaskData.TASK_NAME, jobTaskName);
        jsonTaskData.put(TaskData.TASK_COMMAND, command);
        jsonTaskData.put(TaskData.JOB_DATA, jobAdditionalParam);
        return ByteString.copyFromUtf8(jsonTaskData.toString());
    }

    enum JobState{
        INITIALIZED, RUNNING, PAUSED, STOP, DONE;
    }


    /* List of getter methods that will be called to store the state of this job*/
    public JobState getState(){return state;}

    public long getJobID() {return jobID;}

    public String getJobName() {return jobName;}

    public double getTaskTime(){ return jobTaskTime; }

    public String getJobTaskName() {return jobTaskName;}

    public JSONObject getJobAdditionalParam(){ return jobAdditionalParam; }

    public String getBestLocation() { return jobCurrentBestSolution; }

    public double getBestEnergy() { return jobBestEnergy; }

    public Long getStartingTime() { return jobStartingTime; }

    public JSONArray getEnergyHistory() { return energyHistory; }

    public Protos.ExecutorInfo getTaskExecutor() { return taskExecutor; }

    public int getNumFinishedTasks(){
        if(division_is_done){
            return finishedTasks.cardinality();
        }else{
            return 0;
        }
    }

    public int getNumTasksSent(){ return currentTask;}

    public int getNumTotalTasks() {
        if(division_is_done){
            return returnedResult.length();
        }else{
            return -1; // number of total tasks is unknown, job has not been divided into tasks yet
        }
    }
}
