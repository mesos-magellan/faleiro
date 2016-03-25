package org.magellan.faleiro;

import org.json.JSONObject;
import spark.Request;
import spark.Response;
import spark.Spark;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Web {
    private static final Logger log = Logger.getLogger(Web.class.getName());
    private static MagellanFramework framework;

    public static void main(String[] args) {
        init();
    }

    public static void init() {
        log.log(Level.INFO, "Initializing MagellanFramework");
        framework = new MagellanFramework();
        framework.initializeFramework(System.getenv("MASTER_ADDRESS"));
        framework.startFramework();
        initWebRoutes();
    }

    private static void initWebRoutes() {
        log.log(Level.INFO, "Initializing Spark web routes");
        Spark.post("/api/job", Web::createJob);
        Spark.options("/api/job", Web::createJobOptions);
        Spark.put("/api/job/:job_id/status", Web::updateJobStatus);
        Spark.options("/api/job/:job_id/status", Web::updateJobStatusOptions);
        Spark.get("/api/jobs", Web::getJobList);
        Spark.get("/api/job/:job_id", Web::getJob);
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
        log.log(Level.FINE, "createJob", req);
        res.status(createJobResponse(jsonReq, jsonRes));

        return jsonRes.toString();
    }

    public static Integer createJobResponse(final JSONObject request, JSONObject response) {
        if(request.isNull("job_name")
                || request.isNull("job_time")
                || request.isNull("module_url")) {
            response.put("message", "A parameter is missing");
            log.log(Level.WARNING, "createJobResponse(422) : " + response.getString("message"), request);
            return 422;
        }

        String jobName = request.getString("job_name");
        Integer jobInitTemp = 100;
        Integer jobIterationsPerTemp = 100;
        Double jobInitCoolingRate = 0.1;
        Integer taskTime = request.getInt("job_time");
        String moduleUrl = request.getString("module_url");
        JSONObject jobData = request.optJSONObject("module_data");
        if(jobData == null) {
            jobData = new JSONObject();
        }

        Long jobId = framework.createJob(jobName, jobInitTemp, jobInitCoolingRate, jobIterationsPerTemp
                , taskTime, moduleUrl, jobData);

        if(jobId < 0) {
            response.put("message", "Failed to create job internally");
            log.log(Level.WARNING, "createJobResponse(500) : " + response.getString("message"), request);
            return 500;
        } else {
            response.put("job_id", jobId);
            log.log(Level.FINE, "createJobResponse : Create job ID: " + jobId, request);
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
        log.log(Level.FINE, "updateJobStatus", req);

        if(!req.params().containsKey(":job_id")) {
            jsonRes.put("message", "A parameter is missing");
            log.log(Level.WARNING, "updateJobStatus(422) : " + jsonRes.getString("message"), req);
            res.status(422);
            return jsonRes.toString();
        }

        res.status(updateJobStatusResponse(jsonReq, jsonRes, req.params(":job_id")));

        return jsonRes.toString();
    }

    public static Integer updateJobStatusResponse(final JSONObject request, JSONObject response, String job_id) {
        if(request.isNull("status")) {
            response.put("message", "A parameter is missing");
            log.log(Level.WARNING, "updateJobStatusResponse(422) : " + response.getString("message") + " Job ID : " + job_id, request);
            return 422;
        }

        String status = request.getString("status");
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
                response.put("message", "Invalid parameter value");
                log.log(Level.WARNING, "updateJobStatusResponse(422) : " + response.getString("message") + " Job ID : " + job_id, request);
                return 422;
        }

        log.log(Level.FINE, "updateJobStatusResponse : Job ID: " + jobId + " set to " + status, request);
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
     *     job_starting_temp : int,
     *     job_cooling_rate : double,
     *     job_count : int,
     *     task_starting_temp : int,
     *     task_cooling_rate : double,
     *     task_count : int,
     *     best_location : String,
     *     best_energy : double,
     *     energy_history : [
     *          double
     *     ]
     *   }
     * ]
     */
    private static String getJobList(Request req, Response res) {
        res.type("application/json");
        res.header("Access-Control-Allow-Credentials", "false");
        res.header("Access-Control-Allow-Origin", "*");
        res.header("Access-Control-Allow-Headers", "X-Requested-With, Content-Type");
        res.header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
        log.log(Level.FINE, "getJobList", req);
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
     *     job_starting_temp : int,
     *     job_cooling_rate : double,
     *     job_count : int,
     *     task_starting_temp : int,
     *     task_cooling_rate : double,
     *     task_count : int,
     *     best_location : String,
     *     best_energy : double,
     *     energy_history : [
     *          double
     *     ]
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
        log.log(Level.FINE, "getJob", req);

        if(!req.params().containsKey(":job_id")) {
            JSONObject jsonRes = new JSONObject();
            res.status(422);
            jsonRes.put("message", "A parameter is missing");
            log.log(Level.WARNING, "getJob(422) : " + jsonRes.getString("message"), req);
            return jsonRes.toString();
        }

        JSONObject response = new JSONObject();
        res.status(getJobResponse(response, req.params(":job_id")));
        return response.getJSONObject("response").toString();
    }

    public static Integer getJobResponse(JSONObject response, String job_id) {
        Long jobId = Long.parseLong(job_id);
        Integer status = 200;
        if(!framework.isDone(jobId)) {
            status = 202;
        }
        response.put("response", framework.getSimpleJobStatus(jobId));
        log.log(Level.FINE, "getJobResponse() : Got job details for ID : " + job_id + " status code is " + status , response);
        return status;
    }
}
