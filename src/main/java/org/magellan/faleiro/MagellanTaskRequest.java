package org.magellan.faleiro;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;

import java.util.List;

public class MagellanTaskRequest implements TaskRequest {

    int m_id;
    String m_name;
    double m_cpus;
    double m_netMbps;
    double m_disk;
    int m_ports;
    double m_mem;
    byte[] m_data;

    MagellanTaskRequest(int id,
                        String name,
                        double cpus,
                        double mem,
                        double netMbps,
                        double disk,
                        int ports,
                        byte[] data)
    {
        this.m_id = id;
        m_name = name;
        m_cpus = cpus;
        m_mem = mem;
        m_netMbps = netMbps;
        m_disk = disk;
        m_ports = ports;
        m_data = data;
    }
    @Override
    public String getId() {
        return Integer.toString(m_id);
    }

    @Override
    public String taskGroupName() {
        return m_name;
    }

    @Override
    public double getCPUs() {
        return m_cpus;
    }

    @Override
    public double getMemory() {
        return m_mem;
    }

    @Override
    public double getNetworkMbps() {
        return m_netMbps;
    }

    @Override
    public double getDisk() {
        return m_disk;
    }

    @Override
    public int getPorts() {
        return m_ports;
    }

    @Override
    public List<? extends ConstraintEvaluator> getHardConstraints() {
        return null;
    }

    @Override
    public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
        return null;
    }
}
