package org.magellan.faleiro;

import org.json.JSONObject;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.magellan.faleiro.JsonTags.WebAPI;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class WebTest {

    public void InitWorkingFramework(boolean IsJobDone) {
        MagellanFramework mf = mock(MagellanFramework.class);

        doNothing().when(mf).initializeFramework(anyString());
        doNothing().when(mf).startFramework();
        doReturn(0L).when(mf).createJob(anyString(), anyInt(), anyDouble(), anyInt(), anyInt(), anyString(), anyObject());
        doNothing().when(mf).pauseJob(anyLong());
        doNothing().when(mf).resumeJob(anyLong());
        doNothing().when(mf).stopJob(anyLong());
        doReturn(IsJobDone).when(mf).isDone(anyLong());
        doReturn(new JSONObject()).when(mf).getSimpleJobStatus(anyLong());

        Web.initFramework(mf);
    }

    public void InitFailedFramework() {
        MagellanFramework mf = mock(MagellanFramework.class);

        doNothing().when(mf).initializeFramework(anyString());
        doNothing().when(mf).startFramework();
        doReturn(-1L).when(mf).createJob(anyString(), anyInt(), anyDouble(), anyInt(), anyInt(), anyString(), anyObject());

        Web.initFramework(mf);
    }

    @Test
    public void TestCreateJob() throws Exception {
        InitWorkingFramework(false);
        JSONObject request = new JSONObject();
        JSONObject response = new JSONObject();

        request.put(WebAPI.JOB_NAME, "Test Job");
        request.put(WebAPI.JOB_TIME, 1);
        request.put(WebAPI.MODULE_URL, "traveling-sailor");
        Integer status = Web.createJobResponse(request, response);

        assertTrue(status == 200);
        Integer JobID = response.getInt(WebAPI.JOB_ID);
        assertTrue(JobID == 0);

        InitFailedFramework();
        status = Web.createJobResponse(request, response);

        assertTrue(status == 500);

        InitWorkingFramework(false);
        request = new JSONObject();
        response = new JSONObject();
        status = Web.createJobResponse(request, response);
        assertTrue(status == 422);
    }

    @Test
    public void TestUpdateJobStatus() throws Exception {
        InitWorkingFramework(false);
        JSONObject request = new JSONObject();
        JSONObject response = new JSONObject();

        request.put(WebAPI.STATUS, "pause");
        Integer status = Web.updateJobStatusResponse(request, response, "0");

        assertTrue(status == 200);

        request = new JSONObject();
        request.put(WebAPI.STATUS, "resume");
        status = Web.updateJobStatusResponse(request, response, "0");

        assertTrue(status == 200);

        request = new JSONObject();
        request.put(WebAPI.STATUS, "stop");
        status = Web.updateJobStatusResponse(request, response, "0");

        assertTrue(status == 200);

        request = new JSONObject();
        status = Web.updateJobStatusResponse(request, response, "0");

        assertTrue(status == 422);

        request = new JSONObject();
        request.put(WebAPI.STATUS, "foobar");
        status = Web.updateJobStatusResponse(request, response, "0");

        assertTrue(status == 422);
    }

    @Test
    public void TestGetJob() throws Exception {
        JSONObject response = new JSONObject();

        InitWorkingFramework(false);
        Integer status = Web.getJobResponse(response, "0");
        assert(status == 202);

        InitWorkingFramework(true);
        status = Web.getJobResponse(response, "0");
        assert(status == 200);
    }
}
