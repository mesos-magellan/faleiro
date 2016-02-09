package org.magellan.faleiro;

import org.json.JSONObject;
import org.omg.PortableInterceptor.Interceptor;
import spark.Request;
import spark.Response;
import spark.Spark;

public class Web {

    private static MagellanFramework framework;

    public static void main(String[] args) {
        framework = new MagellanFramework(System.getenv("MASTER_ADDRESS"));
        framework.startFramework();
        initWebRoutes();
    }

    private static void initWebRoutes() {
        Spark.post("/api/job", Web::createJob);
        Spark.put("/api/job/:job_id/status", Web::updateJobStatus);
        Spark.get("/api/jobs", Web::getJobList);
        Spark.get("/api/job/:job_id", Web::getJob);
    }

    /**
     * POST /api/job
     *
     * Request:
     * {
     *     job_name : String,
     *     job_init_temp : int,
     *     job_init_cooling_rate : double,
     *     job_iterations_per_temp : int,
     *     task_time : int,
     *     task_name : String
     *     job_data : JSONObject
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
        JSONObject jsonReq = new JSONObject(req.body());
        JSONObject jsonRes = new JSONObject();
        if(jsonReq.isNull("job_name")
                || jsonReq.isNull("job_init_temp")
                || jsonReq.isNull("job_init_cooling_rate")
                || jsonReq.isNull("job_iterations_per_temp")
                || jsonReq.isNull("task_time")
                || jsonReq.isNull("task_name")) {
            res.status(422);
            jsonRes.put("message", "A parameter is missing");
            return jsonRes.toString();
        }

        String jobName = jsonReq.getString("job_name");
        Integer jobInitTemp = jsonReq.getInt("job_init_temp");
        Integer jobIterationsPerTemp = jsonReq.getInt("job_iterations_per_temp");
        Double jobInitCoolingRate = jsonReq.getDouble("job_init_cooling_rate");
        Integer taskTime = jsonReq.getInt("task_time");
        String taskName = jsonReq.getString("task_name");
        JSONObject jobData = jsonReq.isNull("job_data") ? new JSONObject() : jsonReq.getJSONObject("job_data");

        Long jobId = framework.createJob(jobName, jobInitTemp, jobInitCoolingRate, jobIterationsPerTemp
                , taskTime, taskName, jobData);
        if(jobId < 0) {
            res.status(500);
            jsonRes.put("message", "Failed to create job");
        } else {
            jsonRes.put("job_id", jobId);
        }
        return jsonRes.toString();
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
        JSONObject jsonReq = new JSONObject(req.body());
        JSONObject jsonRes = new JSONObject();
        if(jsonReq.isNull("status") || !req.params().containsKey(":job_id")) {
            res.status(422);
            jsonRes.put("message", "A parameter is missing");
            return jsonRes.toString();
        }

        String status = jsonReq.getString("status");
        Long jobId = Long.parseLong(req.params(":job_id"));

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
                res.status(422);
                jsonRes.put("message", "Invalid parameter value");
                return jsonRes.toString();
        }

        res.status(200);
        return jsonRes.toString();
    }

    /**
     * GET /api/jobs
     *
     * Request:
     * {
     * }
     *
     * Response(200):
     * {
     *  [
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
     *  ]
     * }
     */
    private static String getJobList(Request req, Response res) {
        res.type("application/json");
        return framework.getAllJobStatuses().toString();
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
        if(!req.params().containsKey(":job_id")) {
            JSONObject jsonRes = new JSONObject();
            res.status(422);
            jsonRes.put("message", "A parameter is missing");
            return jsonRes.toString();
        }
        Long jobId = Long.parseLong(req.params(":job_id"));

        if(!framework.isDone(jobId)) {
            res.status(202);
        }

        res.header("Access-Control-Allow-Credentials", "false");
        res.header("Access-Control-Allow-Origin", "*");
        res.header("Access-Control-Allow-Headers", "X-Requested-With, Content-Type");
        res.header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");

        return framework.getJobStatus(jobId).toString();
    }
}
