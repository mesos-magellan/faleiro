package org.magellan.faleiro;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.List;

/**
 * Created by koushik on 12/01/16.
 */
public class MagellanScheduler implements Scheduler {

    public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {

    }

    public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {

    }

    public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> list) {

    }

    public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {

    }

    public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {

    }

    public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {

    }

    public void disconnected(SchedulerDriver schedulerDriver) {

    }

    public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {

    }

    public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {

    }

    public void error(SchedulerDriver schedulerDriver, String s) {

    }
}
