package com.mr.commonFriends;

import com.mr.wc.WordCountDriver;
import com.mr.wc.WordCountMapper;
import com.mr.wc.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2018/3/6.
 */
public class FriendStepOne{
    static class FriendStepOneMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] person_friends = line.split(":");
            String person = person_friends[0];
            String friends = person_friends[1];

            for (String friend : friends.split(",")){
                context.write(new Text(friend),new Text(person));
            }

        }
    }

    static class FriendStepOneReducer extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer(key.toString()).append("\t");
            for (Text person : values){
                sb.append(person).append(",");
            }
            context.write(new Text(sb.toString()), NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 指定本程序的jar包所在的本地路径
        job.setJarByClass(FriendStepOne.class);
        // 指定本业务job使用的mapper/reducer作业
        job.setMapperClass(FriendStepOneMapper.class);
        job.setReducerClass(FriendStepOneReducer.class);

        // 指定mapper输出数据的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path("D:\\data\\input\\friend"));

        // 指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job,new Path("D:\\data\\input\\friendOutput"));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
