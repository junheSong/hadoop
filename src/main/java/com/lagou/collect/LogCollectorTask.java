package com.lagou.collect;

import com.lagou.Singlton.PropTool2;
import com.lagou.common.Constant;
import com.sun.xml.bind.v2.runtime.reflect.opt.Const;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;
import java.util.logging.SimpleFormatter;

public class LogCollectorTask extends TimerTask {
    @Override
    public void run() {

        Properties prop = null;
        try {
            prop = PropTool2.getProp();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //采集的业务逻辑
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-DD");
        String date = simpleDateFormat.format(new Date());
        //扫描指定目录，找到待上传文件
        File logsDir = new File(prop.getProperty(Constant.LOGS_DIR));
        final String log_prefix = prop.getProperty(Constant.LOG_RPEFIX);
        File[] uploadFiles = logsDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(log_prefix);
            }
        });
        //待上传文件转移到临时目录
        //判断临时文件夹是否存在
        File tmpFile = new File(prop.getProperty(Constant.LOG_TMP_FOLDER));
        if (!tmpFile.exists())
            tmpFile.mkdirs();
        for (File uploadFile : uploadFiles) {
            uploadFile.renameTo(new File(tmpFile.getPath() + "/" + uploadFile.getName()));
        }
        //hdfs上传文件至指定目录
        Configuration configuration = new Configuration();
        //configuration.set("fs.defaultFS","hdfs://192.168.80.150:9000");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://192.168.80.150:9000"), configuration, "root");
            File[] files = tmpFile.listFiles();
            //判断hdfs上是否存在文件夹
            Path path = new Path(prop.getProperty(Constant.HDFS_TARGET_FOLDER) + date);
            if (!fs.exists(path))
                fs.mkdirs(path);

            File bakFolder = new File(prop.getProperty(Constant.BAK_FOLDER) + date);
            if (!bakFolder.exists())
                bakFolder.mkdirs();

            for (File file : files) {
                fs.copyFromLocalFile(new Path(file.getPath()),new Path(prop.getProperty(Constant.HDFS_TARGET_FOLDER) + date + "/" + file.getName()));
                //上传后的文件转移到备份目录
                file.renameTo(new File(bakFolder.getPath() + "/" + file.getName()));
            }
        } catch (IOException | URISyntaxException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}
