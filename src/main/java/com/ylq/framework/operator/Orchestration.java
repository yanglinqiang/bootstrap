package com.ylq.framework.operator;

import com.ylq.framework.ILoader;

import java.io.IOException;

/**
 * Created by ylq on 2018/1/12.
 */
public class Orchestration implements ILoader {
    @Override
    public void init() {

    }

    @Override
    public void start() {
        Long start=System.currentTimeMillis();
        Process proc;
        try {
            proc = Runtime.getRuntime().exec("python /Users/ylq/talkingData/DSS_file/20180104Pandas/process.py");
            proc.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("耗时："+(System.currentTimeMillis()-start));
    }
}
