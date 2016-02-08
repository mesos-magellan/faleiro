package org.magellan.faleiro;

import com.google.protobuf.ByteString;
import com.netflix.fenzo.TaskRequest;
import jdk.nashorn.api.scripting.JSObject;
import org.apache.mesos.Protos;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAccumulator;

public class MagellanJob {

    private final double TEMP_MIN = 0.0001;
    private final double NUM_CPU = 1;
    private final double NUM_MEM = 32;
    private final double NUM_NET_MBPS = 0;
    private final double NUM_DISK = 0;
    private final int NUM_PORTS = 0;

    // Unique id to identify job among all other jobs that have been created
    private final long jobID;

    private final String jobName;

    // Current temperature of the job. is temperature is used to choose the starting locations for tasks
    // created by this job. If the temperature is still high, then the job has a greater chance of choosing
    // a worse starting position rather than the current best solution. If the temperature is low, then the
    // job will run tasks closer to the search space where the current best solution was found.
    //
    // NOTE: This temperature is different than the temperature used by the executor. The executor has
    // its own temperature associated with it.
    private double jobTemp;

    // The rate at which the jobTemp variable depreciates.
    // The higher this is, the greater the change that we explore a greater area in our search space
    private double jobCoolingRate;

    // Number of iterations for each decrease in temperature
    // The higher this is, the greater the chance that we thoroughly explore the neighbors around our
    // starting position
    private int jobIterationsPerTemp;

    // Starting temp of new tasks
    private double taskTemp;

    // Cooling rate for tasks
    private double taskCoolingRate;

    // Task nUmber of iterations per temperature
    private double taskCount;

    // The current best solution as determined by all the running tasks
    private String jobCurrentBestSolution = "";

    // The energy of the current best solution
    private double jobBestEnergy = 0;

    private String jobTaskName;

    // A list of the best energies found by every task run by this job.
    private ArrayList<Double> energyHistory = new ArrayList<>();

    // This list stores tasks that are ready to be scheduled. This list is then consumed by the
    // MagellanFramework when it is ready to accept new tasks.
    private BlockingQueue<MagellanTaskRequest> pendingTasks = new LinkedBlockingQueue<>(10);

    // The number of tasks sent out. This will be used as the id for a specific task and along with the
    // job id, will uniquely identify tasks withing the entire system.
    private int numTasksSent = 0;

    private int numFinishedTasks = 0;

    // Number of tasks that have been sent out but have not finished yet
    private AtomicInteger numFreeTaskSlotsLeft = new AtomicInteger(10);

    // Main thread which creates new tasks.
    private Thread jobThread;

    private Protos.ExecutorInfo taskExecutor;

    private JobState state = JobState.INIITIALIZED;

    private JSONObject jobAdditionalParam = null;

    /**
     *
     * @param id Unique Job id
     * @param jName Name of job
     * @param jStartingTemp Starting temperature of job. Higher means job runs for longer
     * @param jCoolingRate Rate at which temperature depreciates each time
     * @param jCount number of iterations per temperature for job
     * @param tTemp Starting temperature of job
     * @param tCoolingRate Cooling rate of task
     * @param tCount Number of iterations per temperature for each task
     * @param pathToExecutor Name of the task we want to execute on the executor side
     * @param jso Additional Job param
     */
    public MagellanJob(long id,
                       String jName,
                       double jStartingTemp,
                       double jCoolingRate,
                       int jCount,
                       double tTemp,
                       double tCoolingRate,
                       double tCount,
                       String pathToExecutor,
                       JSONObject jso)
    {
        jobID = id;
        jobTemp = jStartingTemp;
        jobCoolingRate = jCoolingRate;
        jobIterationsPerTemp = jCount;
        jobName = jName;
        jobTaskName = pathToExecutor;
        taskTemp = tTemp;
        taskCoolingRate = tCoolingRate;
        taskCount = tCount;
        taskExecutor = registerExecutor("/usr/local/bin/enrique");
        jobAdditionalParam = jso;
    }

    public Protos.ExecutorInfo registerExecutor(String pathToExecutor){
        //String uri = "/home/vagrant/" + pathToExecutor;
        System.out.println("Uri for executor is " + pathToExecutor);

        Protos.ExecutorInfo executor = Protos.ExecutorInfo.newBuilder()
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue("default"))
                .setCommand(Protos.CommandInfo.newBuilder().setValue(pathToExecutor))
                .setName("SA Job Executor")
                .setSource("java_test")
                .build();

        return executor;
    }

    /**
     * Returns the job ID
     * @return
     */
    public long getJobID() {
        return jobID;
    }

    /**
     * Returns the name of the job
     * @return
     */
    public String getJobName() {
        return jobName;
    }

    public double getJobStartingTemp(){ return jobTemp;}

    public double getJobCoolingRate(){ return jobCoolingRate; }

    public double getJobCount(){ return jobIterationsPerTemp; }

    public double getTaskStartingTemp() {return taskTemp; }

    public double getTaskCoolingRate(){ return taskCoolingRate; }

    public double getTaskCount(){ return taskCount; }

    public JSONObject getJobAdditionalParam(){ return jobAdditionalParam; }

    public String getBestLocation() { return jobCurrentBestSolution; }

    public double getBestEnergy() { return jobBestEnergy; }

    public ArrayList<Double> getEnergyHistory() { return energyHistory; }

    public Protos.ExecutorInfo getTaskExecutor() { return taskExecutor; }

    public int getNumTasksSent(){ return numTasksSent; }

    public int getNumFinishedTasks(){ return numFinishedTasks;}


    /**
     * Starts the main thread of the job in a separate thread thread.
     */
    public void start() {
        jobThread = new Thread(() -> {
            run();
        });
        state = JobState.RUNNING;
        jobThread.start();
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
        while (jobTemp >= TEMP_MIN) {
            int i = 0;
            while(i < jobIterationsPerTemp) {
                if(state == JobState.STOP) {
                    return;
                }
                while(state==JobState.PAUSED || numFreeTaskSlotsLeft.get() == 0 ){
                    // Waste cycles while job is paused or if we have reached our cap
                    // of availble slots for tasks for this job.
                }
                i++;
                try {
                    // To keep the task ids unique throughout the global job space, use the job ID to
                    // ensure uniqueness
                    String newTaskId = "" + jobID + (numTasksSent+1);

                    // Choose the magellan specific parameters for the new task
                    ByteString data = pickNewTaskStartingLocation(taskTemp, taskCoolingRate, taskCount, newTaskId, jobAdditionalParam);
                    // Create a task request object with parameters that fenzo will be looking for when
                    // pairing mesos resource offers with magellan tasks.
                    MagellanTaskRequest newTask = new MagellanTaskRequest(
                            newTaskId,
                            jobName,
                            NUM_CPU,
                            NUM_MEM,
                            NUM_NET_MBPS,
                            NUM_DISK,
                            NUM_PORTS,
                            data);

                    // Add the task to the pending queue which will be serviced by the magellan framework
                    // when it is ready.
                    pendingTasks.put(newTask);
                    numTasksSent++;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            jobTemp = jobTemp * jobCoolingRate;
        }
        System.out.println("[" + jobID + "]" + " done. Best fitness (" + jobBestEnergy + ") achieved at location " + jobCurrentBestSolution);
    }

    /**
     * Called by magellan framework when a message from the executor is sent to this job. This method
     * processess the message and changes the best location and fitness store if needed.
     * @param o
     */
    public void processIncomingMessages(ByteString o) {
        // Retrieve the data sent by the executor
        JSONObject js = new JSONObject(o);
        String taskID = (String) js.get(MagellanTaskDataJsonTag.UID);
        double fitness_score = (double) js.get(MagellanTaskDataJsonTag.FITNESS_SCORE);
        String best_location = (String) js.get(MagellanTaskDataJsonTag.BEST_LOCATION);

        numFreeTaskSlotsLeft.getAndIncrement();
        numFinishedTasks++;
        energyHistory.add(jobBestEnergy);
        // If a better score was discovered, make this our global, best location
        if(fitness_score > jobBestEnergy) {
            jobCurrentBestSolution = best_location;
        }
        System.out.println("[" + taskID + "] Updated global best. Fitness: " + fitness_score + ". Path: " + best_location);
    }

    public void stop() {
        state = JobState.STOP;
    }

    public void pause() {
        state = JobState.PAUSED;
    }

    public void resume(){
        state = JobState.RUNNING;
    }

    /**
     * Called by the zookeeper service to transfer a snapshot of the current state of the job to save in
     * case this node goes down.
     * @return A snapshot of all the important information in this job
     */
    public JobState getStatus(){
       return state;
    }

    /**
     * Called by the magellan framework to get a list of tasks that this job wants scheduled.
     * @return
     */
    public ArrayList<MagellanTaskRequest> getPendingTasks(){
        ArrayList<MagellanTaskRequest> pt = new ArrayList<>();
        if(numFreeTaskSlotsLeft.get() <= 10) {
            // Get the current value
            int value = numFreeTaskSlotsLeft.get();
            // Set this to zero while we drain our pending tasks so that the main job thread doesn't
            // add new tasks between the time we drain the pendingTasks queue and update this value
            numFreeTaskSlotsLeft.set(0);
            int taken = pendingTasks.drainTo(pt, value);
            // If we did drain 10 tasks, make the correction
            numFreeTaskSlotsLeft.addAndGet(value-taken);
        }
        return pt;
    }


    /**
     * Takes the given parameters and packages it into a json formatted Bytestring which can be
     * packaged into a TaskInfo object by the magellan framework
     * @param temp - Starting temperature of the task
     * @param coolingRate - Rate at which the temperature cools
     * @param count - Number of iterations per temperature
     * @param location - Starting location of the task
     * @param id - ID of the task
     * @return
     */
    private ByteString packTaskData(double temp, double coolingRate, double count, String location, String id, JSONObject job_data){
        JSONObject json = new JSONObject();
        json.put(MagellanTaskDataJsonTag.UID, id);
        json.put(MagellanTaskDataJsonTag.TEMPERATURE, temp);
        json.put(MagellanTaskDataJsonTag.COOLING_RATE, coolingRate);
        json.put(MagellanTaskDataJsonTag.COUNT, count);
        json.put(MagellanTaskDataJsonTag.JOB_DATA, job_data);
        json.put(MagellanTaskDataJsonTag.TASK_NAME, jobTaskName);
        // If location is null, then we want the task to start at a random value.
        if(location!=null) {
            json.put(MagellanTaskDataJsonTag.LOCATION, location);
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
    private ByteString pickNewTaskStartingLocation(double temp, double coolingRate, double count, String taskId, JSONObject job_data){
        String location;
        //if(Math.exp(jobBestEnergy/jobTemp) > Math.random()) {
        if(true)    {
            System.out.println("[" + jobID + "] Picked current best location");
            location = jobCurrentBestSolution;
        } else {
            System.out.println("[" + jobID + "] Picked random location");
            location = "";
        }
        // TODO Need to pick a tempearture here. According to internet this should actually be the cooling rate
        return packTaskData(temp, coolingRate, count, location, taskId, job_data);
    }

    enum JobState{
        INIITIALIZED, RUNNING, PAUSED, STOP;
    }
}
