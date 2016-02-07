package org.magellan.faleiro;

import org.json.JSONObject;
import org.omg.PortableInterceptor.Interceptor;
import spark.Request;
import spark.Response;
import spark.Spark;

public class Web {

    private static MagellanFramework framework;

    public static void main(String[] args) {
        framework = new MagellanFramework("127.0.1.1:5050");
        framework.startFramework();
        initWebRoutes();
    }

    private static void initWebRoutes() {
        Spark.post("/api/job", Web::createJob);
        Spark.put("/api/job/*/status", Web::updateJobStatus);
        Spark.patch("/api/job/*", Web::modifyJob);
        Spark.delete("/api/job", Web::destroyJob);
        Spark.get("/api/jobs", Web::getJobList);
        Spark.get("/api/job/*", Web::getJob);
        Spark.get("/api/job/*/tasks", Web::getJobTaskList);
        Spark.get("/api/job/*/task/*", Web::getJobTask);
    }

    /** POST /api/job
     *
     * Request:
     * {
     *     job_name : String,
     *     init_temp : int,
     *     init_cooling_rate : double,
     *     iterations_per_temp : int,
     *     docker_name : String
     * }
     * // Job successfully created
     * Response(200):
     * {
     *     job_id : int
     * }
     * // Missing parameter
     * Response(422):
     * {
     *     message : String
     * }*/
    private static String createJob(Request req, Response res) {
        res.type("application/json");
        JSONObject jsonReq = new JSONObject(req.body());
        JSONObject jsonRes = new JSONObject();
        if(jsonReq.isNull("job_name")
           || jsonReq.isNull("init_temp")
           || jsonReq.isNull("init_cooling_rate")
           || jsonReq.isNull("iterations_per_temp")
           || jsonReq.isNull("docker_name")) {
            res.status(422);
            jsonRes.put("message", "A parameter is missing");
            return jsonRes.toString();
        }

        String jobName = jsonReq.getString("job_name");
        String dockerName = jsonReq.getString("docker_name");
        Integer initTemp = jsonReq.getInt("init_temp");
        Integer iterationsPerTemp = jsonReq.getInt("iterations_per_temp");
        Double initCoolingRate = jsonReq.getDouble("init_cooling_rate");

        long jobId = framework.createJob(jobName, initTemp, initCoolingRate, iterationsPerTemp);
        if(jobId < 0) {
            res.status(500);
            jsonRes.put("message", "Failed to create job");
        } else {
            jsonRes.put("job_id", jobId);
        }
        return jsonRes.toString();
    }

    private static String updateJobStatus(Request req, Response res) {
        throw new UnsupportedOperationException();
    }

    private static String modifyJob(Request req, Response res) {
        throw new UnsupportedOperationException();
    }

    private static String destroyJob(Request req, Response res) {
        throw new UnsupportedOperationException();
    }

    private static String getJobList(Request req, Response res) {
        throw new UnsupportedOperationException();
    }

    private static String getJob(Request req, Response res) {
        throw new UnsupportedOperationException();
    }

    private static String getJobTaskList(Request req, Response res) {
        throw new UnsupportedOperationException();
    }

    private static String getJobTask(Request req, Response res) {
        throw new UnsupportedOperationException();
    }
}
