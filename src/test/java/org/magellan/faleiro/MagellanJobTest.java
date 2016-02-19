package org.magellan.faleiro;

import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by koushik on 19/02/16.
 */
public class MagellanJobTest {


    MagellanJob testBeginning;
    MagellanJob testMiddle;
    MagellanJob testEnd;

    boolean initialized = false;

    @org.junit.Before
    public void setUp() throws Exception {
        testBeginning = new MagellanJob(3,                  // Id
                                        "tester",           // Job name
                                        10,                 // Starting Temp
                                        0.5,                // Cooling rate
                                        2,                  // Iterations per temp
                                        10,                 // Task time
                                        "task_tester",      // task name
                                        null);              // additional parameters

        if(!initialized) {
            testMiddle = new MagellanJob(3,                  // Id
                    "tester",           // Job name
                    10,                 // Starting Temp
                    0.5,                // Cooling rate
                    2,                  // Iterations per temp
                    10,                 // Task time
                    "task_tester",      // task name
                    null);              // additional parameters
            testMiddle.start();

            testEnd = new MagellanJob(3,                  // Id
                    "tester",           // Job name
                    10,                 // Starting Temp
                    0.5,                // Cooling rate
                    2,                  // Iterations per temp
                    10,                 // Task time
                    "task_tester",      // task name
                    null);              // additional parameters

        }
        initialized = true;

    }

    @org.junit.After
    public void tearDown() throws Exception {

    }

    @Test
    public void testRegisterExecutor() throws Exception {

    }

    @Test
    public void testGetPendingTasks() throws Exception {
        testBeginning.start();

        //Give the job some time to run
        try{Thread.sleep(100);}catch(InterruptedException ie){}

        // First call to getPendingTasks should return 10 tasks
        assertEquals(testBeginning.getPendingTasks().size(),10);
        // Second call to getPendingTasks should return 0 tasks
        assertEquals(testBeginning.getPendingTasks().size(),0);

        //Create fake results
        JSONObject j = new JSONObject();
        j.put("best_location", "india");
        j.put("fitness_score", "88843.54807018393");
        j.put("uid",4);
        // Pas results to job
        testBeginning.processIncomingMessages(j.toString());
        //Give the job some time to update internal state
        try{Thread.sleep(100);}catch(InterruptedException ie){}

        //Job should have one more task ready for scheduling
        assertEquals(testBeginning.getPendingTasks().size(),1);
    }


    @Test
    public void testIsDone() throws Exception {
        assertFalse(testBeginning.isDone());
    }

    @Test
    public void testProcessIncomingMessages() throws Exception {

    }

    @Test
    public void testStop() throws Exception {
        assertEquals(testBeginning.getState(),MagellanJob.JobState.INITIALIZED);
        testBeginning.stop();
        assertEquals(testBeginning.getState(),MagellanJob.JobState.STOP);
    }

    @Test
    public void testPause() throws Exception {
        assertEquals(testBeginning.getState(),MagellanJob.JobState.INITIALIZED);
        testBeginning.pause();
        assertEquals(testBeginning.getState(),MagellanJob.JobState.PAUSED);
    }

    @Test
    public void testResume() throws Exception {
        assertEquals(testBeginning.getState(),MagellanJob.JobState.INITIALIZED);
        testBeginning.resume();
        assertEquals(testBeginning.getState(),MagellanJob.JobState.RUNNING);
    }

    @Test
    public void testGetStateSnapshot() throws Exception {

    }

    @Test
    public void testGetClientFriendlyStatus() throws Exception {
        //assertTrue(false);
    }

    @Test
    public void testGetState() throws Exception {
        assertEquals(testBeginning.getState(),MagellanJob.JobState.INITIALIZED);
    }

    @Test
    public void testGetJobID() throws Exception {
        assertEquals(testBeginning.getJobID(),3,0);
    }

    @Test
    public void testGetJobName() throws Exception {
        assertTrue(testBeginning.getJobName().equals("tester"));
    }

    @Test
    public void testGetJobStartingTemp() throws Exception {
        assertEquals(testBeginning.getJobStartingTemp(),10,0);
    }

    @Test
    public void testGetJobCoolingRate() throws Exception {
        assertEquals(testBeginning.getJobCoolingRate(),0.5,0);
    }

    @Test
    public void testGetJobIterations() throws Exception {
        assertEquals(testBeginning.getJobIterations(),2,0);
    }

    @Test
    public void testGetTaskTime() throws Exception {
        assertEquals(testBeginning.getTaskTime(),10,0);
    }

    @Test
    public void testGetJobTaskName() throws Exception {
        assertTrue(testBeginning.getJobTaskName().equals("task_tester"));
    }

    @Test
    public void testGetJobAdditionalParam() throws Exception {
        assertEquals(testBeginning.getJobAdditionalParam(),null);
    }

    @Test
    public void testGetBestLocation() throws Exception {
        assertTrue(testBeginning.getBestLocation().equals(""));
    }

    @Test
    public void testGetBestEnergy() throws Exception {
        assertEquals(testBeginning.getBestEnergy(), Double.MAX_VALUE, 0);
    }

    @Test
    public void testGetEnergyHistory() throws Exception {
        assertEquals(testBeginning.getEnergyHistory().size(),0);
    }


    @Test
    public void testGetNumTasksSent() throws Exception {
        assertEquals(testBeginning.getNumTasksSent(),0);
    }

    @Test
    public void testGetNumFinishedTasks() throws Exception {
        assertEquals(testBeginning.getNumFinishedTasks(),0);
    }

    @Test
    public void testGetNumTotalTasks() throws Exception {
        assertEquals(testBeginning.getNumTotalTasks(),40);
    }
}