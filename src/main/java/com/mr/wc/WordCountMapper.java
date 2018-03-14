package com.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * KEYIN : 默认情况下,map框架读到的一行文本的起始偏移量
 *VALUEIN ：默认情况下，是mr框架所读到的一行文本内容，String,用Text
 * Created by Administrator on 2018/3/4.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    /**
     * map阶段的业务逻辑就写在自定义的map方法中
     * maptask会对每一行输入数据调用一次我们自定义的map方法
     * @param key
     * @param value
     * @param context
     */
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        // 将maptask传给我们的文本内容先转换成String
        String line = value.toString();
        String[] words = line.split(" ");

        // 将单词输出为<单词，1>
        for (String word : words){
            // 将单词作为key,次数作为value,以便于后续的数据分发
            // 根据单词分发，便于相同单词到相同的reduce task
            context.write(new Text(word),new IntWritable(1));
        }
    }

}
