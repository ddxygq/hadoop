package com.mr.exercise;

import org.apache.hadoop.conf.Configuration;
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
public class FlowCountProvience {
    static class FlowCountProvienceMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] fields = line.split("\t");

            if(fields.length > 2){

                String phone = fields[1];

                long upFlow = Long.parseLong(fields[fields.length - 3]);
                long downFlow = Long.parseLong(fields[fields.length - 2]);
                long sumFlow = upFlow + downFlow;

                context.write(new Text(phone),new FlowBean(upFlow,downFlow));
            }
        }
    }

    static class FlowCountProvienceReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_downFlow = 0;

            for (FlowBean bean : values){
                sum_upFlow = sum_upFlow + bean.getUpFlow();
                sum_downFlow = sum_downFlow + bean.getDownFlow();
            }

            FlowBean resultBean = new FlowBean(sum_upFlow,sum_downFlow);
            context.write(key,resultBean);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name","local");
        // conf.set("yarn.resourcemanager.hostname","master1.hadoop");
        Job job = Job.getInstance(conf);

        // job.setJar("/home/hadoop");
        // 指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCountProvience.class);
        // 指定本业务job使用的mapper/reducer作业
        job.setMapperClass(FlowCountProvience.FlowCountProvienceMapper.class);
        job.setReducerClass(FlowCountProvience.FlowCountProvienceReducer.class);

        // 指定mapper输出数据的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 指定我们自定义的数据分区数量
        job.setPartitionerClass(ProvincePartitioner.class);

        //指定reducetask
        job.setNumReduceTasks(5);

        // 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        // 指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
