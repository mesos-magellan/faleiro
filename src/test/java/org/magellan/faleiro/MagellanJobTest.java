package org.magellan.faleiro;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.reflect.TypeToken;
import org.apache.mesos.Protos;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedDeque;

import static org.magellan.faleiro.JsonTags.TaskData;
import static org.magellan.faleiro.JsonTags.VerboseStatus;
import static org.magellan.faleiro.JsonTags.SimpleStatus;

import static org.junit.Assert.*;

public class MagellanJobTest {
    MagellanJob testBeginning;

    @org.junit.Before
    public void setUp() throws Exception {
        testBeginning = new MagellanJob(3,                  // Id
                                        "tester",           // Job name
                                        10,                 // Task time
                                        "task_tester",      // task name
                                        null);              // additional parameters

    }

    @org.junit.After
    public void tearDown() throws Exception {

    }

    @Test
    public void testRegisterExecutor() throws Exception {
        Protos.ExecutorInfo executorInfo = testBeginning.registerExecutor(System.getenv("EXECUTOR_PATH"));
        Protos.ExecutorInfo mExecutorInfo = testBeginning.getTaskExecutor();

        // Only really care about location of executor
        assertTrue(mExecutorInfo.getCommand().getValue().equals(executorInfo.getCommand().getValue()));
    }

    //@Test
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

//    @Test
//    public void testProcessIncomingMessages() throws Exception {
//        JSONObject jso = new JSONObject();
//        jso.put(TaskData.FITNESS_SCORE,"1234");
//        jso.put(TaskData.BEST_LOCATION, "1234");
//
//        int prevFinishedTasks = testBeginning.getNumFinishedTasks();
//        int prevHistorySize = testBeginning.getEnergyHistory().size();
//        testBeginning.processIncomingMessages(jso.toString());
//
//        assertEquals(prevFinishedTasks + 1,testBeginning.getNumFinishedTasks());
//        assertEquals(prevHistorySize + 1, testBeginning.getEnergyHistory().size());
//
//        try {
//            testBeginning.processIncomingMessages(null);
//            assertTrue(true);
//        }catch (Exception e){
//            assertTrue(false);
//        }
//    }

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

//    @Test
//    public void testGetStateSnapshot() throws Exception {
//        JSONObject state = testBeginning.getStateSnapshot();
//        assertEquals((Double) state.get(VerboseStatus.CURRENT_ITERATION), 0, 0);
//        assertEquals((Double) state.get(VerboseStatus.CURRENT_TEMP), 10, 0);
//        assertEquals((Integer) state.get(VerboseStatus.NUM_TASKS_SENT), 0, 0);
//        assertEquals((Double) state.get(VerboseStatus.TEMP_MIN), 0, 0);
//        assertEquals((Double) state.get(VerboseStatus.NUM_CPU), 1, 0);
//        assertEquals((Double) state.get(VerboseStatus.NUM_MEM), 32, 0);
//        assertEquals((Double) state.get(VerboseStatus.NUM_NET_MBPS), 0, 0);
//        assertEquals((Double) state.get(VerboseStatus.NUM_DISK), 0, 0);
//        assertEquals((Integer) state.get(VerboseStatus.NUM_PORTS), 0, 0);
//        assertEquals((Integer) state.get(VerboseStatus.NUM_SIMULTANEOUS_TASKS), 10, 0);
//    }

//    @Test
//    public void testGetSimpleStatus() throws Exception {
//        JSONObject simple = testBeginning.getSimpleStatus();
//        assertEquals((Long)simple.get(SimpleStatus.JOB_STARTING_TIME),System.currentTimeMillis(),500);
//        assertEquals((Long)simple.get(SimpleStatus.JOB_ID),3,0);
//        assertEquals((String)simple.get(SimpleStatus.JOB_NAME),"tester");
//        assertEquals((Double) simple.get(SimpleStatus.JOB_COUNT),2.0,0);
//        assertEquals((Double) simple.get(SimpleStatus.TASK_SECONDS),10,0);
//        assertEquals((String)simple.get(SimpleStatus.TASK_NAME),"task_tester");
//        assertEquals((String)simple.get(SimpleStatus.BEST_LOCATION),"");
//
//        JSONArray eh = simple.getJSONArray(SimpleStatus.ENERGY_HISTORY);
//
//        assertTrue(eh.length() == 0);
//        assertEquals((Integer) simple.get(SimpleStatus.NUM_FINISHED_TASKS),0,0);
//
//        try {
//            Double p = (Double) simple.get(SimpleStatus.ADDITIONAL_PARAMS);
//            assertTrue(false);
//        }catch (Exception e){
//            assertTrue(true);
//        }
//
//        assertEquals((MagellanJob.JobState) simple.get(SimpleStatus.CURRENT_STATE), MagellanJob.JobState.INITIALIZED);
//    }



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

//    @Test
//    public void testGetNumTotalTasks() throws Exception {
//        assertEquals(testBeginning.getNumTotalTasks(),40);
//    }

    @Test
    public void testStart() throws Exception {

    }

    @Test
    public void testGetTaskExecutor() throws Exception {

    }
}