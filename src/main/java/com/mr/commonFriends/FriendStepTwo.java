package com.mr.commonFriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Administrator on 2018/3/6.
 */
public class FriendStepTwo {
    static class FriendStepTwoMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] person_friends = line.split("\t");
            String friend = person_friends[0];
            String[] persons = person_friends[1].split(",");

            Arrays.sort(persons);

            for (int i = 0;i < persons.length - 2;i++){
                for (int j = i + 1;j<persons.length - 1;j++){
                    context.write(new Text(persons[i] + "-" +persons[j]),new Text(friend));
                }
            }

        }
    }

    static class FriendStepTwoReducer extends Reducer<Text,Text,Text,NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer(key.toString()).append("\t");
            for (Text friend : values){
                sb.append(friend).append(",");
            }
            context.write(new Text(sb.toString()), NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 指定本程序的jar包所在的本地路径
        job.setJarByClass(FriendStepTwo.class);
        // 指定本业务job使用的mapper/reducer作业
        job.setMapperClass(FriendStepTwoMapper.class);
        job.setReducerClass(FriendStepTwoReducer.class);

        // 指定mapper输出数据的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path("D:\\data\\input\\friendOutput"));

        // 指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job,new Path("D:\\data\\input\\friendOutput2"));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
