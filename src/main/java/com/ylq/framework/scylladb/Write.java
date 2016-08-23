package com.ylq.framework.scylladb;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.ylq.framework.ILoader;
import com.ylq.framework.support.ConfigUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by 杨林强 on 16/8/22.
 */
public class Write implements ILoader {
    private ExecutorService executor;
    private Session[] sessions;
    private Cluster cluster;
    private Integer perThreadNum; //每个线程插入多少数量
    private Long startMax = 0L; //程序启动时数据库中的此表最大的id
    private static final Logger logger = LogManager.getLogger(Write.class);

    @Override
    public void init() {
        cluster = Cluster.builder()
                .addContactPoints(ConfigUtil.getString("scylladb.cluster.ips")).withPort(9042)
                .build();
        perThreadNum = ConfigUtil.getInt("scylladb.per.thread.num");
    }

    @Override
    public void start() {
        logger.info("Start time :"+System.currentTimeMillis());
        setStartMax();
        try {
            Integer num = ConfigUtil.getInt("scylladb.thread.num");
            executor = Executors.newFixedThreadPool(num);
            sessions = new Session[num];
            for (int i = 0; i < num; i++) {
                sessions[i] = cluster.connect();
                Thread threadWork = createWriteThread(i);
                threadWork.setDaemon(true);
                executor.execute(threadWork);
                threadWork.join();
            }
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        } finally {
            for (Session session : sessions) {
                if (!session.isClosed())
                    session.close();
            }
            if (cluster != null) {
                cluster.close();
            }
        }
    }

    private void setStartMax() {
        Session readSession = null;
        try {
            readSession = cluster.connect();
            startMax = readSession.execute("select max(id) as maxIndex from  examples.table1").one().getLong("maxIndex");
        } finally {
            if (readSession != null)
                readSession.close();
        }
    }

    private Thread createWriteThread(final Integer threadIndex) {
        return new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < perThreadNum; i++) {
                    Object[] param = new Object[]{startMax + threadIndex * perThreadNum + i,
                            UUID.randomUUID().toString(),
                            System.currentTimeMillis()
                    };
                    sessions[threadIndex].execute("INSERT INTO examples.table1 (id,text,col_time) VALUES (?,?,?)", param);
                }
            }
        };
    }

}
