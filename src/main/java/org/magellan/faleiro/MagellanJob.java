package org.magellan.faleiro;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.ByteString;
import org.apache.mesos.Protos;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class MagellanJob {

    // These constants are used to tell the framework how much of each
    // resource each task created by this job needs to execute
    private final double TEMP_MIN;
    private final double NUM_CPU;
    private final double NUM_MEM;
    private final double NUM_NET_MBPS;
    private final double NUM_DISK;
    private final int NUM_PORTS;
    private final int NUM_SIMULTANEOUS_TASKS; // The maximum number of tasks that this task can create at a time

    private final long jobID;

    private final String jobName;

    // Current temperature of the job. The temperature is used to choose the starting locations for tasks
    // created by this job. If the temperature is still high, then the job has a greater chance of choosing
    // a worse starting position rather than the current best solution. If the temperature is low, then the
    // job will run tasks closer to the search space where the current best solution was found.
    //
    // NOTE: This temperature is different than the temperature used by the executor. The executor has
    // its own temperature.
    private double jobStartingTemp;

    // The rate at which the jobTemp variable "cools".
    // The higher this is, the greater the change that we explore a greater area in our search space
    private double jobCoolingRate;

    // Number of iterations for each decrease in temperature
    // The higher this is, the greater the chance that we thoroughly explore the neighbors around our
    // starting position
    private int jobIterationsPerTemp;

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
    private ConcurrentLinkedDeque<Double> energyHistory = new ConcurrentLinkedDeque<>();

    // This list stores tasks that are ready to be scheduled. This list is then consumed by the
    // MagellanFramework when it is ready to accept new tasks.
    private BlockingQueue<MagellanTaskRequest> pendingTasks = new LinkedBlockingQueue<>();

    // The number of tasks sent out. This number will be combined with the jobId to create a unique
    // identifier for each task
    private int numTasksSent = 0;

    private int numFinishedTasks = 0;

    private final int numTotalTasks;

    // Each job is limited to sending out a certain number of tasks at a time. Currently, this is
    // hardcoded to 10 but in time, this number should dynamically change depending on the number
    // of jobs running in the system.
    private AtomicInteger numFreeTaskSlotsLeft;

    private JobState state = JobState.INITIALIZED;

    private Protos.ExecutorInfo taskExecutor;

    private double currentTemp;

    private double currentIteration;

    /**
     *
     * @param id Unique Job id
     * @param jName Name of job
     * @param jStartingTemp Starting temperature of job. Higher means job runs for longer
     * @param jCoolingRate Rate at which temperature depreciates each time
     * @param jCount number of iterations per temperature for job
     * @param taskName Name of the task we want to execute on the executor side
     * @param jso Additional Job param
     */
    public MagellanJob(long id,
                       String jName,
                       double jStartingTemp,
                       double jCoolingRate,
                       int jCount,
                       int taskTime,
                       String taskName,
                       JSONObject jso)
    {
        jobID = id;
        jobName = jName;
        jobStartingTemp = jStartingTemp;
        jobCoolingRate = jCoolingRate;
        jobIterationsPerTemp = jCount;
        jobTaskTime = taskTime;
        jobTaskName = taskName;
        jobAdditionalParam = jso;
        taskExecutor = registerExecutor("/usr/local/bin/enrique");
        numTotalTasks = (int)(jStartingTemp/jCoolingRate)*jCount + ((jStartingTemp%jCoolingRate!=0)?1:0);

        TEMP_MIN = 0;
        NUM_CPU = 1;
        NUM_MEM = 32;
        NUM_NET_MBPS = 0;
        NUM_DISK = 0;
        NUM_PORTS = 0;
        NUM_SIMULTANEOUS_TASKS = 10;
        currentTemp = jobStartingTemp;
        currentIteration = 0;

        numFreeTaskSlotsLeft = new AtomicInteger(NUM_SIMULTANEOUS_TASKS);

    }

    /**
     *
     * @param j : JSONObject from zookeeper used for creating a new job on this framework based on
     *            the state of a job from another, deceased framework
     */
    public MagellanJob(JSONObject j){

        //Reload constants
        TEMP_MIN = j.getDouble("temp_min");
        NUM_CPU = j.getDouble("num_cpu");
        NUM_MEM = j.getDouble("num_mem");
        NUM_NET_MBPS = j.getDouble("num_net_mbps");
        NUM_DISK = j.getDouble("num_disk");
        NUM_PORTS = j.getInt("num_ports");
        NUM_SIMULTANEOUS_TASKS = j.getInt("num_simultaneous_tasks");

        currentIteration = j.getDouble("current_iteration");
        jobID = j.getInt("job_id");
        jobName = j.getString("job_name");
        jobStartingTemp = j.getDouble("job_starting_temp");
        jobCoolingRate = j.getDouble("job_cooling_rate");
        jobIterationsPerTemp = j.getInt("job_count");
        jobTaskTime = j.getInt("task_seconds");
        jobTaskName = j.getString("task_name");
        jobCurrentBestSolution = j.getString("best_location");
        jobBestEnergy = j.getDouble("best_energy");
        energyHistory = (new Gson()).fromJson(j.getString("energy_history"), new TypeToken<ConcurrentLinkedDeque<Double>>(){}.getType());
        numFinishedTasks = j.getInt("num_finished_tasks");
        numTasksSent = numFinishedTasks;
        numTotalTasks = j.getInt("num_total_tasks");
        jobAdditionalParam = j.getJSONObject("additional_params");
        state = (new Gson()).fromJson(j.getString("current_state"), JobState.class);
        currentTemp = jobStartingTemp - (numFinishedTasks/jobIterationsPerTemp) * jobCoolingRate;


        numFreeTaskSlotsLeft = new AtomicInteger(NUM_SIMULTANEOUS_TASKS);
        taskExecutor = registerExecutor("/usr/local/bin/enrique");
        System.out.println("Created new job from zookeeper. State is " + state);

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

        while (currentTemp > TEMP_MIN) {
            System.out.println("Temp is " + currentTemp);

            while(currentIteration < jobIterationsPerTemp) {
                if(state == JobState.STOP) {
                    return;
                }
                while(state==JobState.PAUSED || numFreeTaskSlotsLeft.get() == 0 ){
                    // Waste cycles while job is paused or if we have reached our cap
                    // of available slots for tasks for this job.
                }

                try {
                    // To keep the task ids unique throughout the global job space, use the job ID to
                    // ensure uniqueness
                    String newTaskId = "" + jobID + (numTasksSent+1);

                    // Choose the magellan specific parameters for the new task
                    ByteString data = pickNewTaskStartingLocation(jobTaskTime, jobTaskName, newTaskId, jobAdditionalParam);

                    MagellanTaskRequest newTask = new MagellanTaskRequest(
                            newTaskId,
                            jobName,
                            NUM_CPU,
                            NUM_MEM,
                            NUM_NET_MBPS,
                            NUM_DISK,
                            NUM_PORTS,
                            data);

                    // Add the task to the pending queue until the framework requests it
                    pendingTasks.put(newTask);
                    numFreeTaskSlotsLeft.decrementAndGet();
                    numTasksSent++;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                currentIteration++;
            }
            currentIteration = 0;
            // Depreciate the temperature
            currentTemp = currentTemp - jobCoolingRate;
        }
        System.out.println("Finished sending tasks. Waiting now. [Tasks sent, Tasks Finished] = [" + numTasksSent + ","+numFinishedTasks+"]");

        while(state != JobState.STOP && (numTasksSent != numFinishedTasks)) {
            // Waste time while we wait for all tasks to finish
            try{Thread.sleep(100);}catch(InterruptedException ie){}
        }

        System.out.println("[Job " + jobID + "]" + " done. Best fitness (" + jobBestEnergy + ") achieved at location " + jobCurrentBestSolution);
        state=JobState.DONE;
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
     * Called by magellan framework when a message from the executor is sent to this job. This method
     * processess the message and changes the best location and fitness store if needed.
     * @param data
     */
    public void processIncomingMessages(String data) {

        // Retrieve the data sent by the executor
        JSONObject js = new JSONObject(data);
        double fitness_score = js.getDouble(TaskDataJsonTag.FITNESS_SCORE);
        String best_location = js.getString(TaskDataJsonTag.BEST_LOCATION);

        numFreeTaskSlotsLeft.getAndIncrement();
        numFinishedTasks++;
        energyHistory.add(fitness_score);
        // If a better score was discovered, make this our global, best location
        if(fitness_score < jobBestEnergy) {
            jobCurrentBestSolution = best_location;
            jobBestEnergy = fitness_score;
        }
    }

    public void stop() {
        state = JobState.STOP;
    }

    public void pause() {
        if(!isDone()) {
            state = JobState.PAUSED;
        }
    }

    public void resume(){
        if(!isDone()) {
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
        jsonObj.put("current_iteration", currentIteration);
        jsonObj.put("current_temp", currentTemp);
        jsonObj.put("num_tasks_sent", getNumTasksSent());

        // Store constants
        jsonObj.put("temp_min", TEMP_MIN);
        jsonObj.put("num_cpu", NUM_CPU);
        jsonObj.put("num_mem", NUM_MEM);
        jsonObj.put("num_net_mbps", NUM_NET_MBPS);
        jsonObj.put("num_disk", NUM_DISK);
        jsonObj.put("num_ports", NUM_PORTS);
        jsonObj.put("num_simultaneous_tasks", NUM_SIMULTANEOUS_TASKS);

        return  jsonObj;
    }

    /**
     * This method returns information that the client wants to know about the job
     * @return
     */
    public JSONObject getSimpleStatus() {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("job_id", getJobID());
        jsonObj.put("job_name", getJobName());
        jsonObj.put("job_starting_temp", getJobStartingTemp());
        jsonObj.put("job_cooling_rate", getJobCoolingRate());
        jsonObj.put("job_count", getJobIterations());
        jsonObj.put("task_seconds", getTaskTime());
        jsonObj.put("task_name", getJobTaskName());
        jsonObj.put("best_location", getBestLocation());
        jsonObj.put("best_energy", getBestEnergy());
        jsonObj.put("energy_history", new Gson().toJson(getEnergyHistory()));
        jsonObj.put("num_running_tasks", getNumTasksSent() - getNumFinishedTasks());
        jsonObj.put("num_finished_tasks", getNumFinishedTasks());
        jsonObj.put("num_total_tasks", getNumTotalTasks());
        jsonObj.put("additional_params", getJobAdditionalParam());
        jsonObj.put("current_state", getState());
        return jsonObj;
    }

    /**
     * Takes the given parameters and packages it into a json formatted Bytestring which can be
     * packaged into a TaskInfo object by the magellan framework
     * @param taskTime
     * @param taskName
     * @param location
     * @param id
     * @param job_data
     * @return
     */
    private ByteString packTaskData(int taskTime, String taskName, String location, String id, JSONObject job_data){
        JSONObject json = new JSONObject();
        json.put(TaskDataJsonTag.UID, id);
        json.put(TaskDataJsonTag.TASK_SECONDS, taskTime);
        json.put(TaskDataJsonTag.JOB_DATA, job_data);
        json.put(TaskDataJsonTag.TASK_NAME, taskName);
        json.put(TaskDataJsonTag.FITNESS_SCORE, jobBestEnergy);

        // If location is null, then we want the task to start at a random value.
        if(location == jobCurrentBestSolution) {
            json.put(TaskDataJsonTag.FITNESS_SCORE, jobBestEnergy);
            json.put(TaskDataJsonTag.LOCATION, location);
        } else {
            json.put(TaskDataJsonTag.FITNESS_SCORE, "");
            json.put(TaskDataJsonTag.LOCATION, location);
        }
        return ByteString.copyFromUtf8(json.toString());
    }

    /**
     * For every new task created, this function is called to determine its starting location. It uses
     * an acceptance probability to decide whether or not the new task should start at the current best
     * location or at a random location
     *
     * The acceptance probability to use here will simply be e^(h(A)/T) where A is the current best location
     * and h(a) is the current best energy. We will choose the current best location as the starting location
     * of the next task if  exponent < Random number between 0 and 1.
     *
     * If the temperature of the job is still hight, then there will be a greater tendence for the task to start
     * at a random location. As the temeprature decreases, more of the new tasks will start their search at the
     * current best location.
     * @param taskId
     * @return
     */
    private ByteString pickNewTaskStartingLocation(int taskTime, String taskName, String taskId, JSONObject job_data){
        String location;
        /*double lastEnergy;
        if(!energyHistory.isEmpty()) {
            lastEnergy = energyHistory.getLast();
            double df = lastEnergy - jobBestEnergy;
            if(df < 0){
                System.out.println("PICKED BEST LOCATION");
                location = jobCurrentBestSolution;
            }else if(Math.exp(-df/jobTemp) > Math.random()){
                System.out.println("PICKED RANDOM LOCATION");
                location = "";
            }else{
                System.out.println("PICKED BEST LOCATION");
                location = jobCurrentBestSolution;
            }
        } else {
            System.out.println("PICKED RANDOM LOCATION");
            location = "";
        }*/


        /*if(Math.exp(jobBestEnergy/jobTemp) > Math.random()) {*/
        if(true){
            //System.out.println("[" + jobID + "] Picked current best location");
            location = jobCurrentBestSolution;
        } else {
            //System.out.println("[" + jobID + "] Picked random location");
            location = "";
        }

        // TODO Need to pick a temperature here. According to internet this should actually be the cooling rate
        return packTaskData(taskTime, taskName, location, taskId, job_data);
    }

    enum JobState{
        INITIALIZED, RUNNING, PAUSED, STOP, DONE;
    }


    /* List of getter methods that will be called to store the state of this job*/
    public JobState getState(){return state;}

    public long getJobID() {return jobID;}

    public String getJobName() {return jobName;}

    public double getJobStartingTemp(){ return jobStartingTemp;}

    public double getJobCoolingRate(){ return jobCoolingRate; }

    public double getJobIterations(){ return jobIterationsPerTemp; }

    public double getTaskTime(){ return jobTaskTime; }

    public String getJobTaskName() {return jobTaskName;}

    public JSONObject getJobAdditionalParam(){ return jobAdditionalParam; }

    public String getBestLocation() { return jobCurrentBestSolution; }

    public double getBestEnergy() { return jobBestEnergy; }

    public Queue<Double> getEnergyHistory() { return energyHistory; }

    public Protos.ExecutorInfo getTaskExecutor() { return taskExecutor; }

    public int getNumTasksSent(){ return numTasksSent; }

    public int getNumFinishedTasks(){ return numFinishedTasks;}

    public int getNumTotalTasks() {return numTotalTasks;}
}
