package com.lagou.Singlton;

import com.lagou.collect.LogCollectorTask;

import java.io.IOException;
import java.util.Properties;

public class PropTool2 {

    //volatile禁止指令重排序的关键字,保证有序性和可见性
    private static volatile Properties prop = null;

    //线程安全问题
    //懒汉式
    public static synchronized Properties getProp() throws IOException {
        if (prop == null) {
            synchronized ("lock") {
                if (prop == null) {
                    prop = new Properties();
                    prop.load(LogCollectorTask.class.getClassLoader()
                            .getResourceAsStream("collector.properties"));
                }
            }
        }
        return prop;
    }
}
