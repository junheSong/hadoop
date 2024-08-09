package com.lagou.MapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text K = new Text();
    IntWritable V = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取行
        String line = value.toString();
        //行切割
        String[] lines = line.split(" ");
        for (String s : lines) {
                K.set(s);
                context.write(K,V);
        }
    }
}
