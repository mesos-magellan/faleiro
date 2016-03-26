package org.magellan.faleiro;


public abstract class JsonTags {

    // JSON tags used within the Web API
    static class WebAPI {
        public static final String JOB_NAME = "job_name";
        public static final String JOB_TIME = "job_time";
        public static final String MODULE_URL = "module_url";
        public static final String MODULE_DATA = "module_data";
        public static final String JOB_ID = "job_id";
        public static final String MESSAGE = "message";
        public static final String STATUS = "status";
        public static final String RESPONSE = "response";
    }

    // JSON tags for messages passed between the executor and scheduler
    static class TaskData {
        public static final String LOCATION = "location";
        public static final String UID = "uid";
        public static final String TASK_SECONDS = "task_seconds";
        public static final String FITNESS_SCORE = "fitness_score";
        public static final String BEST_LOCATION = "best_location";
        public static final String JOB_DATA = "job_data";
        public static final String TASK_NAME = "name";
        public static final String TASK_COMMAND = "command";
        public static final String TASK_DIVISIONS = "divisions";
        public static final String PROBLEM_DATA = "problem_data";
    }

    // JSON tags for information desired by the client that is related to the status/progress
    // of the job.
    static class SimpleStatus {
        public static final String JOB_ID = "job_id";
        public static final String JOB_NAME = "job_name";
        public static final String JOB_STARTING_TEMP = "job_starting_temp";
        public static final String JOB_COOLING_RATE = "job_cooling_rate";
        public static final String JOB_COUNT = "job_count";
        public static final String JOB_STARTING_TIME = "job_starting_time";
        public static final String TASK_SECONDS = "task_seconds";
        public static final String TASK_NAME = "task_name";
        public static final String BEST_LOCATION = "best_location";
        public static final String BEST_ENERGY = "best_energy";
        public static final String ENERGY_HISTORY = "energy_history";
        public static final String NUM_RUNNING_TASKS = "num_running_tasks";
        public static final String NUM_FINISHED_TASKS = "num_finished_tasks";
        public static final String NUM_TOTAL_TASKS = "num_total_tasks";
        public static final String ADDITIONAL_PARAMS = "additional_params";
        public static final String CURRENT_STATE = "current_state";
    }

    // JSON tags used for persisting internal state of each job in zookeeper
    static class VerboseStatus {
        public static final String CURRENT_ITERATION = "current_iteration";
        public static final String CURRENT_TEMP = "current_temp";
        public static final String NUM_TASKS_SENT = "num_tasks_sent";
        public static final String TEMP_MIN = "temp_min";
        public static final String NUM_CPU = "num_cpu";
        public static final String NUM_MEM = "num_mem";
        public static final String NUM_NET_MBPS = "num_net_mbps";
        public static final String NUM_DISK = "num_disk";
        public static final String NUM_PORTS = "num_ports";
        public static final String NUM_SIMULTANEOUS_TASKS = "num_simultaneous_tasks";
    }

}
