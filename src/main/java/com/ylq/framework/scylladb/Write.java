package com.ylq.framework.scylladb;

import com.datastax.driver.core.Session;
import com.ylq.framework.ILoader;
import com.ylq.framework.support.ConfigUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 杨林强 on 16/8/22.
 */
public class Write extends Scylla implements ILoader {
    private AtomicLong indexAll;
    private static final Logger logger = LogManager.getLogger(Write.class);

    @Override
    public void init() {
        initScylla();
    }

    @Override
    public void start() {
        Long startMax = getMaxId();
        logger.info("The max of examples.table1.id:{}", startMax);
        Long seeds = Long.valueOf(ConfigUtil.getString("scylladb.write.seeds"));
        logger.info("The seeds:{}", seeds);
        indexAll = new AtomicLong(startMax + seeds);
        logger.info("The IndexAll of examples.table1.id:{}", indexAll);
        totalNum += indexAll.get();
        printQps(indexAll.get());//打印qps
        startWork();
        updateMaxId();
        if (cluster != null) {
            cluster.close();
        }
        logger.info("exit app!!");
    }

    private void updateMaxId() {
        Session updateSession = null;
        try {
            updateSession = cluster.connect("examples");
            updateSession.execute(" UPDATE table_index SET max_id = ?  WHERE id=?", totalNum, tableIndex);
            logger.info("update table_index:{}", totalNum);
        } finally {
            if (updateSession != null)
                updateSession.close();
        }
    }

    @Override
    protected Thread createThread(final int threadIndex) {
        return new Thread() {
            Session session = sessions[threadIndex];
            String sql = "INSERT INTO table" + tableIndex + " (id,text,col_time) VALUES (?,?,?)";

            @Override
            public void run() {
                long index = indexAll.getAndIncrement();
                while (index <= totalNum) {
                    Object[] param = new Object[]{index,
                            UUID.randomUUID().toString(),
                            System.currentTimeMillis()
                    };
                    try {
                        session.execute(sql, param);
                        if (index % 10000L == 0) {
                            logger.info(String.join(",", param[0].toString(), param[1].toString(), param[2].toString()));
                            printQps(index);
                        }
                    } catch (Exception ex) {
                        logger.error(ex.getMessage(), ex);
                        session.close();
                        break;
                    }
                    index = indexAll.getAndIncrement();
                }
            }
        };
    }
}
