package com.ylq.framework.scylladb;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.ylq.framework.support.ConfigUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by 杨林强 on 2016/11/14.
 */
public abstract class Scylla {
    protected ExecutorService executor;
    protected Session[] sessions;
    protected Cluster cluster;
    protected Integer threadNum;
    protected Long totalNum; //总的次数

    private Long qpsTime = System.currentTimeMillis();
    private Long lastIndex = 0l;
    protected int tableIndex = 1;
    private static final Logger logger = LogManager.getLogger(Scylla.class);


    protected synchronized void printQps(Long index) {
        Long now = System.currentTimeMillis();
        Long diff = now - qpsTime;
        if (diff > 60000) {//一分钟
            logger.info("qps:{}", ((index - lastIndex) * 1000) / diff);
            lastIndex = index;
            qpsTime = now;
        }
    }

    protected void initScylla() {
        String nodes[] = ConfigUtil.getString("scylladb.cluster.ips").split(",");
        InetSocketAddress address[] = new InetSocketAddress[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            address[i] = new InetSocketAddress(nodes[i].split(":")[0], Integer.valueOf(nodes[i].split(":")[1]));
        }
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, 1, 1)
                .setConnectionsPerHost(HostDistance.REMOTE, 1, 1)
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
                .setMaxRequestsPerConnection(HostDistance.REMOTE, 2000);
        cluster = Cluster.builder()
                .addContactPointsWithPorts(address)
                .build();

        threadNum = ConfigUtil.getInt("scylladb.thread.num");
        logger.info("The num of write Thread:{}", threadNum);
        executor = Executors.newFixedThreadPool(threadNum);
        totalNum = Long.valueOf(ConfigUtil.getString("scylladb.total.num"));
        logger.info("总数量:{} ", totalNum);
        tableIndex = ConfigUtil.getInt("scylladb.table.index");
        sessions = new Session[threadNum];
        for (int i = 0; i < threadNum; i++) {
            sessions[i] = cluster.connect("examples");
        }
    }

    protected Long getMaxId() {
        Session readSession = null;
        try {
            readSession = cluster.connect("examples");
            Long maxId = readSession.execute("SELECT max_id FROM table_index  where id =?", tableIndex).one().getLong("max_id");
            return maxId == null ? 0L : maxId;
        } finally {
            if (readSession != null)
                readSession.close();
        }
    }

    protected void startWork() {
        Long start = System.currentTimeMillis();
        logger.info("start time :{}", new Date(start));
        try {
            for (int i = 0; i < threadNum; i++) {
                Thread threadWork = createThread(i);
                threadWork.setDaemon(true);
                executor.execute(threadWork);
            }
            executor.shutdown();
            executor.awaitTermination(24, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        } finally {
            for (Session session : sessions) {
                if (!session.isClosed())
                    session.close();
            }
        }
        Long time = (System.currentTimeMillis() - start) / 1000;
        Long totalInsert = Long.valueOf(ConfigUtil.getString("scylladb.total.num"));
        logger.info("data:{},time(s):{},qps:{}", totalInsert, time, totalInsert / time);
    }

    protected abstract Thread createThread(final int threadIndex);
}
