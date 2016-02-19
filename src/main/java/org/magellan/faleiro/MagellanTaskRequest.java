package org.magellan.faleiro;

import com.google.protobuf.ByteString;
import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;

import java.util.List;

public class MagellanTaskRequest implements TaskRequest {

    private String m_id;
    private String m_name;
    private double m_cpus;
    private double m_netMbps;
    private double m_disk;
    private int m_ports;
    private double m_mem;
    private ByteString m_data;

    MagellanTaskRequest(String  id,
                        String name,
                        double cpus,
                        double mem,
                        double netMbps,
                        double disk,
                        int ports,
                        ByteString data)
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

    public ByteString getData() { return m_data; };

    @Override
    public String getId() {
        return m_id;
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
