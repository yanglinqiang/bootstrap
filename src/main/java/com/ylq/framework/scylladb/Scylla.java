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
        String nodes[] = ConfigUtil.getString("scylladb.cluster.ips").split(",");
        InetSocketAddress address[] = new InetSocketAddress[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            address[i] = new InetSocketAddress(nodes[i].split(":")[0], Integer.valueOf(nodes[i].split(":")[1]));
        }
        PoolingOptions poolingOptions = new PoolingOptions();
        threadNum = ConfigUtil.getInt("scylladb.thread.num");
        Integer coreNum = ConfigUtil.getInt("scylladb.pooling.core.num");
        Integer perRequest = (threadNum / coreNum) + coreNum;


        poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, coreNum, coreNum)
                .setMaxRequestsPerConnection(HostDistance.LOCAL, perRequest)
                .setNewConnectionThreshold(HostDistance.LOCAL, 100);


        DCAwareRoundRobinPolicy.Builder policyBuilder = DCAwareRoundRobinPolicy.builder();
        policyBuilder.withLocalDc("datacenter1");

        cluster = Cluster.builder()
                .addContactPoint(nodes[1].split(":")[0])
                .withPort(Integer.valueOf(nodes[1].split(":")[1]))
                .withPoolingOptions(poolingOptions)
                .withProtocolVersion(ProtocolVersion.V3)
                .withLoadBalancingPolicy(policyBuilder.build())
                .withoutMetrics()
                .withCompression(ProtocolOptions.Compression.NONE)
                .build();
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
