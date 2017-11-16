package com.ylq.framework.scylladb;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.ylq.framework.ILoader;
import com.ylq.framework.support.ConfigUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 杨林强 on 16/8/22.
 */
public class Write extends Scylla implements ILoader {
    private static final Logger logger = LogManager.getLogger(Write.class);
    private static final String tableName = ConfigUtil.getString("scylladb.table.name");
    private static String seeds;
    private static PreparedStatement prepared;

    private static AtomicLong atomicLong = new AtomicLong(0);
    private static Long max = Long.parseLong(ConfigUtil.getString("scylladb.Write.Max"));

    @Override
    public void init() {
        initScylla();
    }

    @Override
    public void start() {
        getSeeds();
        prepared = session.prepare("UPDATE  \"" + tableName + "\" SET \"col_time\"=? where \"uuid\"=?");
        startWork();
        logger.info("app start!!");

    }

    private void getSeeds() {
        seeds = UUID.randomUUID().toString();
        String sql = "INSERT INTO data_log(seeds,table_name,max_num,create_time) VALUES (?,?,?,?);";
        Object[] param = new Object[]{seeds, tableName, 0l, System.currentTimeMillis()};
        session.execute(sql, param);
        logger.info("insert seeds:{}", seeds);
    }

    @Override
    protected Thread createThread() {
        return new Thread() {
            @Override
            public void run() {
                long index = atomicLong.getAndIncrement();
                while (!Thread.currentThread().isInterrupted() && index < max) {
                    prepared.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
                    try {
                        session.execute(prepared.bind(index, seeds + index));
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                    if (index % 50000 == 0) {
                        logger.info(seeds + index);
                    }
                    index = atomicLong.getAndIncrement();
                }
            }
        };
    }

    @Override
    protected void close() {
        String sql = "UPDATE data_log set max_num=? where seeds=?;";
        Object[] param = new Object[]{atomicLong.get(), seeds};
        session.execute(sql, param);
    }
}
