package com.ylq.framework.scylladb;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.ylq.framework.support.ConfigUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;

/**
 * Created by 杨林强 on 2016/11/14.
 */
public abstract class Scylla {
    protected Thread[] threads;
    protected Session session;
    protected Cluster cluster;
    protected Integer threadNum;


    private static final Logger logger = LogManager.getLogger(Scylla.class);

    protected void initScylla() {
        String nodes[] = ConfigUtil.getString("scylladb.cluster.ips").split(",");

        threadNum = ConfigUtil.getInt("scylladb.thread.num");

        PoolingOptions poolingOpts = new PoolingOptions();
        poolingOpts.setCoreConnectionsPerHost(HostDistance.LOCAL, 8);

        Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPoint(nodes[0].split(":")[0])
                .withPort(9042)
                .withPoolingOptions(poolingOpts)
                .withoutMetrics(); // The driver uses
        clusterBuilder.withCompression(ProtocolOptions.Compression.NONE);
        cluster = clusterBuilder.build();
        Metadata metadata = cluster.getMetadata();
        logger.info("Connected to cluster: {}", metadata.getClusterName());
        for (Host host : metadata.getAllHosts())
        {
            logger.info("Datatacenter: {}; Host: {}; Rack: {}", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
        SimpleStatement stmt = new SimpleStatement("USE \"examples\"; ");
        stmt.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        session.execute(stmt);
        threadNum = ConfigUtil.getInt("scylladb.thread.num");
        logger.info("The num of Thread:{}", threadNum);
        threads = new Thread[threadNum];
    }

    protected void startWork() {
        startShutdownHook();
        for (int i = 0; i < threadNum; i++) {
            threads[i] = createThread();
            threads[i].setDaemon(true);
            threads[i].start();
        }
    }

    private void startShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                for (Thread thread : threads) {
                    thread.interrupt();
                }
                logger.info("close all session!");
                close();
                if (session != null && !session.isClosed()) {
                    session.close();
                }
                if (cluster != null) {
                    cluster.close();
                    logger.info("close cluster!");
                }
                logger.info("exit app!!");
            }
        });
    }

    //关闭时调用
    protected void close() {
    }

    protected abstract Thread createThread();
}
