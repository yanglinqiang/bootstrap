package com.ylq.framework;

/**
 * Created by 杨林强 on 16/8/22.
 */
public abstract class AbstractAppWait {
    private static volatile boolean running = true;

    /**
     * 调用此方法,使程序无限等待下去
     */
    public void appWait() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                synchronized (AbstractAppWait.class) {
                    running = false;
                    AbstractAppWait.class.notify();
                }
            }
        });

        synchronized (AbstractAppWait.class) {
            while (running) {
                try {
                    AbstractAppWait.class.wait();
                } catch (Throwable e) {
                }
            }
        }
        shutDown();
    }

    protected abstract void shutDown();
}
