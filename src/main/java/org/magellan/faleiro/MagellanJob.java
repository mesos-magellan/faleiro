package org.magellan.faleiro;

import com.netflix.fenzo.TaskRequest;

import java.util.concurrent.BlockingQueue;

public class MagellanJob {

    private final long jobID;

    public MagellanJob(long id){
        jobID = id;
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

    public BlockingQueue<TaskRequest> getPendingTasks(){
        throw new UnsupportedOperationException();
    }

    static enum JobState{
        RUNNING, PAUSED, STOP;
    }

}
