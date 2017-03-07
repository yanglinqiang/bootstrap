package com.ylq.framework.scylladb;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.ylq.framework.ILoader;
import com.ylq.framework.support.ConfigUtil;
import org.apache.commons.lang3.RandomUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 杨林强 on 16/10/24.
 */
public class Read extends Scylla implements ILoader {
    private Double hitRate = Double.valueOf(ConfigUtil.getString("scylladb.read.hit.rate"));
    private static final Logger logger = LogManager.getLogger(Read.class);
    private static final String tableName = ConfigUtil.getString("scylladb.table.name");
    private static String[] seedsString;
    private static Long[] seedsMax;
    private static PreparedStatement prepared;


    @Override
    public void init() {
        initScylla();
    }

    @Override
    public void start() {
        Map<String, Long> seedsMap = new HashMap<>();
//        try (Session session = cluster.connect("examples")) {
        String sql = "select * from data_log;";
        List<Row> rowList = session.execute(sql).all();
        for (Row row : rowList) {
            if (row.get("table_name", String.class).equalsIgnoreCase(tableName)) {
                Long max = row.get("max_num", Long.class);
                max = BigDecimal.valueOf(max).divideToIntegralValue(BigDecimal.valueOf(hitRate)).longValue();
                seedsMap.put(row.get("seeds", String.class), max);
            }
        }

//        }
        if (seedsMap.size() == 0) {
            logger.info("获取种子信息失败！");
            return;
        }

        seedsString = seedsMap.keySet().toArray(new String[]{});
        seedsMax = seedsMap.values().toArray(new Long[]{});
        prepared = session.prepare("select * from data1 where uuid=?");
        startWork();
        logger.info("app start!!");
    }

    private String getKey() {
        Integer index = RandomUtils.nextInt(0, seedsString.length);
        return seedsString[index] + RandomUtils.nextLong(0, seedsMax[index]);
    }

    @Override
    protected Thread createThread() {

        return new Thread() {
            @Override
            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    Row row = session.execute(prepared.bind(getKey())).one();
                    if (row != null && row.get("col_time", Long.class) % 5000 == 0) {
                        logger.info(row.get("uuid", String.class));
                    }
                }
            }
        };
    }
}
