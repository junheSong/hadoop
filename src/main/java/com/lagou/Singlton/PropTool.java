package com.lagou.Singlton;

import com.lagou.collect.LogCollectorTask;

import java.io.IOException;
import java.util.Properties;

public class PropTool {
    //类加载时初始化执行一次
    //是由静态代码块实现
    private static Properties prop = null;

    //饿汉式
    static {
        prop = new Properties();
        try {
            prop.load(LogCollectorTask.class.getClassLoader().getResourceAsStream("collector.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProp()
    {
        return prop;
    }
}
