package org.magellan.faleiro;

import org.json.JSONObject;
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
        Spark.post("/api/job", (req, res) -> createJob(req, res));
        Spark.put("/api/job/*/status", (req, res) -> updateJobStatus(req, res));
        Spark.patch("/api/job/*", (req, res) -> modifyJob(req, res));
        Spark.delete("/api/job", (req, res) -> destroyJob(req, res));
        Spark.get("/api/jobs", (req, res) -> getJobList(req, res));
        Spark.get("/api/job/*", (req, res) -> getJob(req, res));
        Spark.get("/api/job/*/tasks", (req, res) -> getJobTaskList(req, res));
        Spark.get("/api/job/*/task/*", (req, res) -> getJobTask(req, res));
    }

    private static String createJob(Request req, Response res) {
        JSONObject jsonRes = new JSONObject();
        long jobId = framework.createJob();
        if(jobId < 0) {
            jsonRes.put("message", "Failed to create job");
        } else {
            jsonRes.put("job_id", jobId);
            jsonRes.put("message", "Job created successfully");
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
