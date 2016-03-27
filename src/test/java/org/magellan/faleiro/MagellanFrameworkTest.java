package org.magellan.faleiro;

import org.json.JSONObject;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;


public class MagellanFrameworkTest {
    @Mock private MagellanJob mockJob;
    //@Mock private MagellanFramework.MagellanEnvVar mockEnvVariables;

    private MagellanFramework mockFramework;

    @org.junit.Before
    public void setUp() throws Exception {
        // Create a framework reference using mockito. This will be used throughout the tests
        //mockFramework = Mockito.mock(MagellanFramework.class);


        //mockJob = Mockito.mock(MagellanJob.class);
        //Mockito.when(new MagellanJob())
        //mockEnvVariables = Mockito.mock(MagellanFramework.MagellanEnvVar.class);

        // When the constructor to MagellanJob is called with any parameters,
        // return the mock instance of MagellanJob
        /*Mockito.when(new MagellanJob(Mockito.any(Long.class),
                                        Mockito.any(String.class),
                                        Mockito.any(Integer.class),
                                        Mockito.any(Double.class),
                                        Mockito.any(Integer.class),
                                        Mockito.any(Integer.class),
                                        Mockito.any(String.class),
                                        Mockito.any(JSONObject.class)))
                .thenReturn(mockJob);*/
        //Mockito.when(mockEnvVariables.getEnv("FRAMEWORK_USER")).thenReturn("magellan");

        // Set the environment variables for the test

        mockFramework = new MagellanFramework();
    }

    @org.junit.After
    public void tearDown() throws Exception {

    }

    @org.junit.Test
    public void testShutdownFramework() throws Exception {

    }

    @org.junit.Test
    public void testStartFramework() throws Exception {

    }

    @org.junit.Test
    public void testCreateJob() throws Exception {
        //Test that an invalid parameter causes failure
        assertEquals(mockFramework.createJob(null, 100, "", new JSONObject()),-1);

        // Test that correct parameters give correct id
        assertEquals(mockFramework.createJob("", 100, "", new JSONObject()),0);

        // Make sure that id numbers are incrementing
        assertEquals(mockFramework.createJob("", 100, "", new JSONObject()),1);
    }

    @org.junit.Test
    public void testStopJob() throws Exception {

    }

    @org.junit.Test
    public void testPauseJob() throws Exception {

    }

    @org.junit.Test
    public void testResumeJob() throws Exception {

    }

    @org.junit.Test
    public void testRunFramework() throws Exception {

    }

    @org.junit.Test
    public void testRecoverTaskId() throws Exception {

    }

    @org.junit.Test
    public void testGetJobStatus() throws Exception {

    }

    @org.junit.Test
    public void testIsDone() throws Exception {

    }

    @org.junit.Test
    public void testGetAllJobStatuses() throws Exception {

    }
}