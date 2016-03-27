package org.magellan.faleiro;

import org.json.JSONObject;
import spark.Request;
import spark.Response;
import spark.Spark;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.magellan.faleiro.JsonTags.WebAPI;

public class Web {
    private static final Logger log = Logger.getLogger(Web.class.getName());
    private static MagellanFramework framework;

    public static void main(String[] args) {
        MagellanFramework mf = new MagellanFramework();
        initFramework(mf);
        initWebRoutes();
    }

    public static void initFramework(MagellanFramework mf) {
        log.log(Level.INFO, "Initializing MagellanFramework");
        framework = mf;
        framework.initializeFramework(System.getenv("MASTER_ADDRESS"));
        framework.startFramework();
    }

    private static void initWebRoutes() {
        log.log(Level.INFO, "Initializing Spark web routes");
        Spark.post("/api/job", Web::createJob);
        Spark.options("/api/job", Web::createJobOptions);
        Spark.put("/api/job/:" + WebAPI.JOB_ID + "/status", Web::updateJobStatus);
        Spark.options("/api/job/:" + WebAPI.JOB_ID + "/status", Web::updateJobStatusOptions);
        Spark.get("/api/jobs", Web::getJobList);
        Spark.get("/api/job/:" + WebAPI.JOB_ID, Web::getJob);
    }

    /**
     * POST /api/job
     *
     *
     * Request:
     * {
     *     job_name : String,
     *     job_time : int,
     *     module_url : String
     *     module_data : JSONObject
     * }
     *
     * // Job successfully created
     * Response(200):
     * {
     *     job_id : int
     * }
     * // Missing parameter
     * Response(422):
     * {
     *     message : String
     * }
     * // Failure to create job on scheduler side
     * Response(500):
     * {
     *     message : String
     * }
     */
    private static String createJob(Request req, Response res) {
        res.type("application/json");
        res.header("Access-Control-Allow-Credentials", "false");
        res.header("Access-Control-Allow-Origin", "*");
        res.header("Access-Control-Allow-Headers", "X-Requested-With, Content-Type");
        res.header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");

        JSONObject jsonReq = new JSONObject(req.body());
        JSONObject jsonRes = new JSONObject();
        log.log(Level.FINE, req.toString(), req);
        res.status(createJobResponse(jsonReq, jsonRes));

        return jsonRes.toString();
    }

    public static Integer createJobResponse(final JSONObject request, JSONObject response) {
        if(request.isNull(WebAPI.JOB_NAME)
                || request.isNull(WebAPI.JOB_TIME)
                || request.isNull(WebAPI.MODULE_URL)) {
            response.put(WebAPI.MESSAGE, "A parameter is missing");
            log.log(Level.WARNING, "(422) : " + response.getString(WebAPI.MESSAGE), request);
            return 422;
        }

        String jobName = request.getString(WebAPI.JOB_NAME);
        Integer jobTime = request.getInt(WebAPI.JOB_TIME);
        String moduleUrl = request.getString(WebAPI.MODULE_URL);
        JSONObject moduleData = request.optJSONObject(WebAPI.MODULE_DATA);
        if(moduleData == null) {
            moduleData = new JSONObject();
        }

        Long jobId = framework.createJob(jobName, jobTime, moduleUrl, moduleData);

        if(jobId < 0) {
            response.put(WebAPI.MESSAGE, "Failed to create job internally");
            log.log(Level.WARNING, "(500) : " + response.getString(WebAPI.MESSAGE), request);
            return 500;
        } else {
            response.put(WebAPI.JOB_ID, jobId);
            log.log(Level.FINE, "Create job ID: " + jobId, request);
            return 200;
        }
    }

    private static String createJobOptions(Request req, Response res) {
        res.type("application/json");
        res.header("Access-Control-Allow-Credentials", "false");
        res.header("Access-Control-Allow-Origin", "*");
        res.header("Access-Control-Allow-Headers", "X-Requested-With, Content-Type");
        res.header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT, OPTIONS");
        return "{}";
    }

    /**
     * PUT /api/job/{job_id}/status
     *
     * Request:
     * {
     *     status : ENUM("resume", "pause", "stop")
     * }
     *
     * Response(200)
     * {
     *
     * }
     * // Missing or Invalid parameter
     * Response(422):
     * {
     *     message : String
     * }
     */
    private static String updateJobStatus(Request req, Response res) {
        res.type("application/json");
        res.header("Access-Control-Allow-Credentials", "false");
        res.header("Access-Control-Allow-Origin", "*");
        res.header("Access-Control-Allow-Headers", "X-Requested-With, Content-Type");
        res.header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");

        JSONObject jsonReq = new JSONObject(req.body());
        JSONObject jsonRes = new JSONObject();
        log.log(Level.FINE, req.toString(), req);

        if(!req.params().containsKey(":" + WebAPI.JOB_ID)) {
            jsonRes.put(WebAPI.MESSAGE, "A parameter is missing");
            log.log(Level.WARNING, "(422) : " + jsonRes.getString(WebAPI.MESSAGE), req);
            res.status(422);
            return jsonRes.toString();
        }

        res.status(updateJobStatusResponse(jsonReq, jsonRes, req.params(":" + WebAPI.JOB_ID)));

        return jsonRes.toString();
    }

    public static Integer updateJobStatusResponse(final JSONObject request, JSONObject response, String job_id) {
        if(request.isNull(WebAPI.STATUS)) {
            response.put(WebAPI.MESSAGE, "A parameter is missing");
            log.log(Level.WARNING, "(422) : " + response.getString(WebAPI.MESSAGE) + " Job ID : " + job_id, request);
            return 422;
        }

        String status = request.getString(WebAPI.STATUS);
        Long jobId = Long.parseLong(job_id);

        switch (status) {
            case "resume":
                framework.resumeJob(jobId);
                break;
            case "pause":
                framework.pauseJob(jobId);
                break;
            case "stop":
                framework.stopJob(jobId);
                break;
            default:
                response.put(WebAPI.MESSAGE, "Invalid parameter value");
                log.log(Level.WARNING, "(422) : " + response.getString(WebAPI.MESSAGE) + " Job ID : " + job_id, request);
                return 422;
        }

        log.log(Level.FINE, "Job ID: " + jobId + " set to " + status, request);
        return 200;
    }

    private static String updateJobStatusOptions(Request req, Response res) {
        res.type("application/json");
        res.status(200);
        res.header("Access-Control-Allow-Credentials", "false");
        res.header("Access-Control-Allow-Origin", "*");
        res.header("Access-Control-Allow-Headers", "X-Requested-With, Content-Type");
        res.header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT, OPTIONS");
        return "{}";
    }

    /**
     * GET /api/jobs
     *
     * Request:
     * {
     * }
     *
     * Response(200):
     * [
     *   {
     *     job_id : int,
     *     job_name : String,
     *     job_starting_time : int,
     *     task_name : String,
     *     task_seconds : int,
     *     current_state : String,
     *     best_location : String,
     *     best_energy : double,
     *     num_finished_tasks : int,
     *     num_total_tasks : int,
     *     energy_history : [
     *          double
     *     ],
     *     additional_params : {
     *         JSON
     *     }
     *   }
     * ]
     */
    private static String getJobList(Request req, Response res) {
        res.type("application/json");
        res.header("Access-Control-Allow-Credentials", "false");
        res.header("Access-Control-Allow-Origin", "*");
        res.header("Access-Control-Allow-Headers", "X-Requested-With, Content-Type");
        res.header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
        log.log(Level.FINE, req.toString(), req);
        return framework.getSimpleAllJobStatuses().toString();
    }

    /**
     * GET /api/job/{job_id}
     *
     * Request:
     * {
     * }
     *
     * Response(200): // job done
     * Response(202): // job still running
     * {
     *     job_id : int,
     *     job_name : String,
     *     job_starting_time : int,
     *     task_name : String,
     *     task_seconds : int,
     *     current_state : String,
     *     best_location : String,
     *     best_energy : double,
     *     num_finished_tasks : int,
     *     num_total_tasks : int,
     *     energy_history : [
     *          double
     *     ],
     *     additional_params : {
     *         JSON
     *     }
     * }
     * // Missing or Invalid parameter
     * Response(422):
     * {
     *     message : String
     * }
     */
    private static String getJob(Request req, Response res) {
        res.type("application/json");
        res.header("Access-Control-Allow-Credentials", "false");
        res.header("Access-Control-Allow-Origin", "*");
        res.header("Access-Control-Allow-Headers", "X-Requested-With, Content-Type");
        res.header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
        log.log(Level.FINE, req.toString(), req);

        if(!req.params().containsKey(":" + WebAPI.JOB_ID)) {
            JSONObject jsonRes = new JSONObject();
            res.status(422);
            jsonRes.put(WebAPI.MESSAGE, "A parameter is missing");
            log.log(Level.WARNING, "(422) : " + jsonRes.getString(WebAPI.MESSAGE), req);
            return jsonRes.toString();
        }

        JSONObject response = new JSONObject();
        res.status(getJobResponse(response, req.params(":" + WebAPI.JOB_ID)));
        return response.getJSONObject(WebAPI.RESPONSE).toString();
    }

    public static Integer getJobResponse(JSONObject response, String job_id) {
        Long jobId = Long.parseLong(job_id);
        Integer status = 200;
        if(!framework.isDone(jobId)) {
            status = 202;
        }
        response.put(WebAPI.RESPONSE, framework.getSimpleJobStatus(jobId));
        log.log(Level.FINE, "Got job details for ID : " + job_id + " status code is " + status , response);
        return status;
    }
}
