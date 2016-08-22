package com.ylq.framework.support;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

import java.util.Properties;


/**
 * Created by 杨林强 on 16/6/17.
 */
public class ConfigUtil extends PropertyPlaceholderConfigurer {
    private static Properties properties;

    @Override
    protected void processProperties(ConfigurableListableBeanFactory beanFactoryToProcess, Properties props) throws BeansException {
        super.processProperties(beanFactoryToProcess, props);
        ConfigUtil.properties = props;
    }
    /**
     * 获取配置信息
     *
     * @param key
     * @return
     */
    public static String getString(String key) {
        return properties.getProperty(key);
    }

    /**
     * @return
     */
    public static int getInt(String key) {
        return Integer.parseInt(getString(key));
    }
}
