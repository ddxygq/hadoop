package com.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 相当于一个yarn集群的客户端
 * 需要在此封装我们的mr程序的相关运行参数，指定jar包
 * 最后提交给yarn
 * Created by Administrator on 2018/3/4.
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // 默认在本地(local)、(file:///)运行，其实不用配置
        // conf.set("mapreduce.framework.name","local");
        // conf.set("fs.defaultFS","file:///");

        // 本地连接集群运行，集群模式参数，如果打包上集群是不用设置这些参数的
        // conf.set("mapreduce.framework.name","yarn");
        // conf.set("yarn.resourcemanager.hostname","master1.hadoop");
        // conf.set("fs.defaultFS","hdfs://master1.hadoop:9000");
        Job job = Job.getInstance(conf);

        // job.setJar("/home/hadoop/jar包名");  可以用java -jar 包名 运行
        // job.setJar("D:\\WordCount.jar");
        // 指定本程序的jar包所在的本地路径
        job.setJarByClass(WordCountDriver.class);
        // 指定本业务job使用的mapper/reducer作业
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 指定mapper输出数据的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Combiner 不能影响最后的结果，比如平均值时候
        job.setCombinerClass(WordCountReducer.class);

        // 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        // 指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
