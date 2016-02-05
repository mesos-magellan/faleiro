package org.magellan.faleiro;

import com.netflix.fenzo.TaskRequest;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MagellanJob {

    // Unique id to identify job among all other jobs that have been created
    private final long jobID;

    // Current temperature of the job. is temperature is used to choose the starting locations for tasks
    // created by this job. If the temperature is still high, then the job has a greater chance of choosing
    // a worse starting position rather than the current best solution. If the temperature is low, then the
    // job will run tasks closer to the search space where the current best solution was found.
    //
    // NOTE: This temperature is different than the temperature used by the executor. The executor has
    // its own temperature associated with it.
    private int jobTemp;

    // The rate at which the jobTemp variable depreciates.
    private double jobCoolingRate;

    // The data from finished tasks are passed from the MagellanFramework and submitted into this queue
    // which can be quired whenever the current job is available to process the responses.
    private BlockingQueue<String> completedTasks = new LinkedBlockingQueue<>();

    // This list stores tasks that are ready to be scheduled. This list is then consumed by the
    // MagellanFramework when it is ready to accept new tasks.
    private BlockingQueue<String> pendingTasks = new LinkedBlockingQueue<>();

    // The number of tasks sent out. This will be used as the id for a specific task and along with the
    // job id, will uniquely identify tasks withing the entire system.
    private int numTasksSent = 0;


    public MagellanJob(long id, int startingTemp, double startingCoolingRate){
        jobID = id;
        jobTemp = startingTemp;
        startingCoolingRate = startingCoolingRate;
    }

    public void start() {
        throw new UnsupportedOperationException();
    }

    public void stop() {
        throw new UnsupportedOperationException();
    }

    public void pause() {
        throw new UnsupportedOperationException();
    }

    public void resume(){
        throw new UnsupportedOperationException();
    }

    public JobState getStatus(){
        throw new UnsupportedOperationException();
    }

    public ArrayList<TaskRequest> getPendingTasks(){
        ArrayList<String> pt = new ArrayList<>();
        pendingTasks.drainTo(pt);
        throw new UnsupportedOperationException();
    }

    static enum JobState{
        RUNNING, PAUSED, STOP;
    }

}
