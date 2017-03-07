package com.ylq.framework.scylladb;

import com.datastax.driver.core.*;
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
        InetSocketAddress address[] = new InetSocketAddress[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            address[i] = new InetSocketAddress(nodes[i].split(":")[0], Integer.valueOf(nodes[i].split(":")[1]));
        }
        PoolingOptions poolingOptions = new PoolingOptions();
        Integer coreNum = ConfigUtil.getInt("scylladb.pooling.core.num");
        Integer maxNum = ConfigUtil.getInt("scylladb.pooling.max.num");
        Integer perRequest = ConfigUtil.getInt("scylladb.connect.request.num");
        poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, coreNum, maxNum)
                .setConnectionsPerHost(HostDistance.REMOTE, coreNum, maxNum)
                .setMaxRequestsPerConnection(HostDistance.LOCAL, perRequest)
                .setMaxRequestsPerConnection(HostDistance.REMOTE, 2000);
        cluster = Cluster.builder()
                .addContactPointsWithPorts(address)
                .withPoolingOptions(poolingOptions)
                .withProtocolVersion(ProtocolVersion.V3)
                .withNettyOptions(new NettyOptions())
                .withLoadBalancingPolicy(new RoundRobinPolicy())
                .build();
        session = cluster.connect("examples");
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
