package com.ylq.framework.scylladb;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.ylq.framework.ILoader;
import com.ylq.framework.support.ConfigUtil;
import org.apache.commons.lang3.RandomUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 杨林强 on 16/10/24.
 */
public class Read extends Scylla implements ILoader {
    private Double hitRate = 0.7;
    private Long idRange = 0L;
    private AtomicLong indexAll = new AtomicLong(0L);
    private static final Logger logger = LogManager.getLogger(Read.class);

    @Override
    public void init() {
        initScylla();
        hitRate = Double.valueOf(ConfigUtil.getString("scylladb.read.hit.rate"));
    }

    @Override
    public void start() {
        idRange = BigDecimal.valueOf(getMaxId()).divideToIntegralValue(BigDecimal.valueOf(hitRate)).longValue();
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
            String sql = "select id,text,col_time from  table" + tableIndex + " where id=?";

            @Override
            public void run() {
                Long index = indexAll.getAndIncrement();
                while (index <= totalNum) {
                    Long id = RandomUtils.nextLong(0, idRange);
                    try {
                        Row row = session.execute(sql, id).one();
                        if (index % 10000L == 0) {
                            logger.info((row == null ? "null" : row.toString()) + index);
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
