package com.mr.exercise;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2018/3/5.
 */
public class FlowCountSort {
    static class FlowCountSortMapper extends Mapper<LongWritable,Text,FlowBean,Text>{

        FlowBean bean = new FlowBean();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] fields = line.split("\t");

            String phone = fields[0];

            long upFlow = Long.parseLong(fields[1]);
            long downFlow = Long.parseLong(fields[2]);

            bean.set(upFlow,downFlow);
            v.set(phone);

            context.write(bean,v);

        }
    }

    static class FlowCountSortReducer extends Reducer<FlowBean,Text,Text,FlowBean>{
        @Override
        protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(),key);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // conf.set("mapreduce.framework.name","local");
        // conf.set("yarn.resourcemanager.hostname","master1.hadoop");
        Job job = Job.getInstance(conf);

        // job.setJar("/home/hadoop");
        // 指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCountSort.class);
        // 指定本业务job使用的mapper/reducer作业
        job.setMapperClass(FlowCountSort.FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSort.FlowCountSortReducer.class);

        // 指定mapper输出数据的KV类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        // 指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        Path outPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outPath)){
            fs.delete(outPath);
        }

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
