package com.ylq.framework.scylladb;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.ylq.framework.ILoader;
import com.ylq.framework.support.ConfigUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 杨林强 on 16/8/22.
 */
public class Write extends Scylla implements ILoader {
    private static AtomicLong indexAll;
    private static final Logger logger = LogManager.getLogger(Write.class);
    private static final String tableName = ConfigUtil.getString("scylladb.table.name");
    private static String seeds;
    private static PreparedStatement prepared;

    @Override
    public void init() {
        initScylla();
    }

    @Override
    public void start() {
        getSeeds();
        indexAll = new AtomicLong(1);
        prepared = session.prepare("INSERT INTO " + tableName + " (uuid,col_time) VALUES (?,?)");
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
                while (!Thread.currentThread().isInterrupted()) {
                    Long index = indexAll.getAndIncrement();
//                    session.executeAsync(prepared.bind(seeds + index, System.currentTimeMillis()));
                    session.execute(prepared.bind(seeds + index, System.currentTimeMillis()));
                    if (index % 50000L == 0) {
                        logger.info(seeds + index);
                    }
                }
            }
        };
    }

    @Override
    protected void close() {
        String sql = "UPDATE data_log set max_num=? where seeds=?;";
        Object[] param = new Object[]{indexAll.get(), seeds};
        session.execute(sql, param);
    }
}
