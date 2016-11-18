package com.ylq.framework.scylladb;

import com.datastax.driver.core.Session;
import com.ylq.framework.ILoader;
import org.apache.commons.lang3.RandomUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 杨林强 on 16/10/24.
 */
public class Update extends Scylla implements ILoader {
    private Long idRange = 0L;
    private AtomicLong indexAll = new AtomicLong(0L);
    private static final Logger logger = LogManager.getLogger(Update.class);

    @Override
    public void init() {
        initScylla();
    }

    @Override
    public void start() {
        idRange = getMaxId();
        logger.info("The id range for select :{}", idRange);
        startWork();
        printQps(0L);
        if (cluster != null) {
            cluster.close();
        }
    }


    @Override
    protected Thread createThread(final int threadIndex) {

        return new Thread() {
            Session session = sessions[threadIndex];
            String sql = "UPDATE table" + tableIndex + " SET text = ?  WHERE id=?";

            @Override
            public void run() {
                Long index = indexAll.getAndIncrement();
                while (index <= totalNum) {
                    Long id = RandomUtils.nextLong(0, idRange);
                    try {
                        Object[] param = new Object[]{UUID.randomUUID().toString(), id};
                        session.execute(sql, param);
                        if (index % 10000 == 0) {
                            logger.info(String.join(",", param[0].toString(), param[1].toString(), index.toString()));
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
