package com.lagou.collect;

import java.util.Timer;

public class LogCollector {
    /*
- 定时采集已滚动完毕日志文件
- 将待采集文件上传到临时目录
- 备份日志文件
*/
    public static void main(String[] args) {
        Timer timer = new Timer();
        //定时采集任务的调度
        timer.schedule(new LogCollectorTask(), 0,3600*1000);
    }
}
