package com.ylq.framework;

import com.ylq.framework.support.ConfigUtil;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by 杨林强 on 16/8/22.
 */
public class Bootstrap {

    /**
     * 程序启动入口
     * @param args
     */
    public static void main(String[] args) {
        new ClassPathXmlApplicationContext("/ApplicationContext.xml");
        ILoader start = null;
        try {
            start = (ILoader) Class.forName(ConfigUtil.getString("strap.class.name")).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        start.init();
        start.start();
    }
}
