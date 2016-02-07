package org.magellan.faleiro;

import com.google.protobuf.ByteString;
import com.netflix.fenzo.TaskRequest;
import jdk.nashorn.api.scripting.JSObject;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MagellanJob {

    private final double TEMP_MIN = 0.00001;
    private final double NUM_CPU = 1;
    private final double NUM_MEM = 128;
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

    // The current best solution as determined by all the running tasks
    private String jobCurrentBestSolution = "";

    // The energy of the current best solution
    private double jobBestEnergy = 0;

    // The data from finished tasks are passed from the MagellanFramework and submitted into this queue
    // which can be queried whenever the current job is available to process the responses.
    private BlockingQueue<ByteString> receivedMessages = new LinkedBlockingQueue<>();

    // This list stores tasks that are ready to be scheduled. This list is then consumed by the
    // MagellanFramework when it is ready to accept new tasks.
    private BlockingQueue<MagellanTaskRequest> pendingTasks = new LinkedBlockingQueue<>(10);

    // The number of tasks sent out. This will be used as the id for a specific task and along with the
    // job id, will uniquely identify tasks withing the entire system.
    private int numTasksSent = 0;

    private JobState state = JobState.INIITIALIZED;

    public MagellanJob(long id, String name, double startingTemp, double coolingRate, int iterationsPerTemp){
        jobID = id;
        jobTemp = startingTemp;
        jobCoolingRate = coolingRate;
        jobIterationsPerTemp = iterationsPerTemp;
        jobName = name;
    }

    public void start() {

        while (jobTemp >= TEMP_MIN) {
            for(int i = 0; i< jobIterationsPerTemp ; i++) {
                try {
                    // First take care of all the message that have been sent back as it is detrimental to get the
                    // current best
                    while(!receivedMessages.isEmpty()) {
                        processMessages(receivedMessages.take());
                    }
                    String newTaskId = "" + jobID + (++numTasksSent);

                    ByteString data = pickNewTaskData(newTaskId);
                    MagellanTaskRequest newTask = new MagellanTaskRequest(
                                                    newTaskId,
                                                    jobName,
                                                    NUM_CPU,
                                                    NUM_MEM,
                                                    NUM_NET_MBPS,
                                                    NUM_DISK,
                                                    NUM_PORTS,
                                                    data);


                    pendingTasks.put(newTask);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
            jobTemp = jobTemp*jobCoolingRate;
        }
        throw new UnsupportedOperationException();
    }

    public void processMessages(ByteString o) {

    }

    public void stop() {
        throw new UnsupportedOperationException();
    }

    public void pause() {
        state = JobState.PAUSED;
        throw new UnsupportedOperationException();
    }

    public void resume(){
        state = JobState.RUNNING;
        throw new UnsupportedOperationException();
    }

    public JobState getStatus(){
        throw new UnsupportedOperationException();
    }

    public ArrayList<MagellanTaskRequest> getPendingTasks(){
        ArrayList<MagellanTaskRequest> pt = new ArrayList<>();
        pendingTasks.drainTo(pt);
        return pt;
    }


    /**
     * Takes the given parameters and packages it into a json formatted Bytestring which can be
     * packaged into a TaskInfo object by the magellan framework
     * @param temp - Starting temperature of the task
     * @param location - Starting location of the task
     * @param id - ID of the task
     * @return
     */
    private ByteString packTaskData(double temp, String location, String id){
        JSONObject json = new JSONObject();
        json.put(MagellanTaskDataJsonTag.UID, id);
        json.put(MagellanTaskDataJsonTag.TEMPERATURE, temp);
        json.put(MagellanTaskDataJsonTag.LOCATION, location);
        return ByteString.copyFromUtf8(json.toString());
    }

    private ByteString pickNewTaskData(String taskId){
        // The acceptance probability to use here will simply be e^(h(A)/T) where A is the current best location
        // and h(a) is the current best energy. We will choose the current best location as the starting location
        // of the next task if the exponent < Random number
        String location;
        if(Math.exp(jobBestEnergy/jobTemp) > Math.random()) {
            System.out.println("[" + jobID + "] Picked current best location");
            location = jobCurrentBestSolution;
        } else {
            System.out.println("[" + jobID + "] Picked random location");
            location = "null";
        }

        return packTaskData(jobTemp, location, taskId);
    }

    enum JobState{
        INIITIALIZED, RUNNING, PAUSED, STOP;
    }
}
