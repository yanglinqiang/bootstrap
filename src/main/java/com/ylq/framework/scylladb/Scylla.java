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

    protected volatile boolean sleeped = false;


    private static final Logger logger = LogManager.getLogger(Scylla.class);

    protected void initScylla() {
//        String nodes[] = ConfigUtil.getString("scylladb.cluster.ips").split(",");
//        InetSocketAddress address[] = new InetSocketAddress[nodes.length];
//        for (int i = 0; i < nodes.length; i++) {
//            address[i] = new InetSocketAddress(nodes[i].split(":")[0], Integer.valueOf(nodes[i].split(":")[1]));
//        }
        threadNum = ConfigUtil.getInt("scylladb.thread.num");
//        Integer coreNum = ConfigUtil.getInt("scylladb.pooling.core.num");

        PoolingOptions poolingOptions = new PoolingOptions();

        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 8);



        cluster = Cluster.builder()
                .addContactPoint("172.23.12.25")
                .withPort(9042)
                .withPoolingOptions(poolingOptions)
                .withoutMetrics()
                .withCompression(ProtocolOptions.Compression.NONE)
                .build();
        Metadata metadata = cluster.getMetadata();
        logger.info("Connected to cluster: %s%n",
                metadata.getClusterName());
        for (Host host : metadata.getAllHosts())
        {
            logger.info("Datatacenter: %s; Host: %s; Rack: %s%n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
        SimpleStatement stmt = new SimpleStatement("USE \"keyspace1\"; ");
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
        while (true) {
            try {
                if (sleeped) {
                    sleep();
                } else {
                    Thread.sleep(500);
                }
            } catch (InterruptedException e) {
                logger.info(e.getMessage(), e);
            }
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

    private void sleep() {
//        for (Thread thread : threads) {
//            thread.interrupt();
//        }
        try {
            logger.info("sleep 15s !");
            Thread.sleep(25000);
        } catch (InterruptedException e) {
            logger.info(e.getMessage(), e);
        }
        truncateTable();
        sleeped = false;
        for (int i = 0; i < threadNum; i++) {
            threads[i] = createThread();
            threads[i].setDaemon(true);
            threads[i].start();
        }
    }

    //关闭时调用
    protected void truncateTable() {

    }

    //关闭时调用
    protected void close() {
    }

    protected abstract Thread createThread();
}
