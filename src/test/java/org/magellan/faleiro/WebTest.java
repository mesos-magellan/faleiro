package org.magellan.faleiro;

import org.json.JSONObject;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.*;

/**
 * Created by dylan on 2016-03-22.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class WebTest {

    private Integer JobID;

    public WebTest() {
        Web.init();
        JobID = -1;
    }

    @Test
    public void TestCreateJob() throws Exception {
        JSONObject request = new JSONObject();
        JSONObject response = new JSONObject();

        request.put("job_name", "Test Job");
        request.put("job_time", 1);
        request.put("module_url", "traveling-sailor");
        request.put("job_data", new JSONObject().put("test-data", 12309134));
        Integer status = Web.createJobResponse(request, response);

        assertTrue(status == 200);
        JobID = response.getInt("job_id");
        assertTrue(JobID >= 0);
    }

    @Test
    public void TestUpdateJobStatusPause() throws Exception {
        JSONObject request = new JSONObject();
        JSONObject response = new JSONObject();

        request.put("status", "pause");
        Integer status = Web.updateJobStatusResponse(request, response, JobID.toString());

        assertTrue(status == 200);
    }

    @Test
    public void TestUpdateJobStatusResume() throws Exception {
        JSONObject request = new JSONObject();
        JSONObject response = new JSONObject();

        request.put("status", "resume");
        Integer status = Web.updateJobStatusResponse(request, response, JobID.toString());

        assertTrue(status == 200);
    }

    @Test
    public void TestUpdateJobStatusStop() throws Exception {
        JSONObject request = new JSONObject();
        JSONObject response = new JSONObject();

        request.put("status", "stop");
        Integer status = Web.updateJobStatusResponse(request, response, JobID.toString());

        assertTrue(status == 200);
    }

    @Test
    public void TestGetJob() throws Exception {
        TestCreateJob();
        JSONObject response = new JSONObject();
        Integer status = Web.getJobResponse(response, JobID.toString());
        assert(status == 200 || status == 202);
        Integer resJobID = response.getJSONObject("response").getInt("job_id");
        assertTrue(resJobID.equals(JobID));
    }
}
