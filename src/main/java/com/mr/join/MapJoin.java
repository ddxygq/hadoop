package com.mr.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2018/3/6.
 */
public class MapJoin {

    static class MapJoinMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

        Map<String,String> pdMap = new HashMap<>();

        Text k = new Text();

        /**
         * setup 方法是在maptask处理数据之前调用一次
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("pdts.txt")));
            String line;
            while (StringUtils.isNotBlank(line = br.readLine())){
                System.out.println(line);
                String[] fields = line.split(",");
                pdMap.put(fields[0],fields[1]);
            }

            br.close();
        }


        // 由于已经持有完整的产品信息表，所以在map方法中就能实现join逻辑了
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String orderLine = value.toString();
            String[] fields = orderLine.split("\t");
            String pdName = pdMap.get(fields[1]);
            k.set(orderLine + "\t" + pdName);
            context.write(k,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(MapJoin.class);

        job.setMapperClass(MapJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 输入数据
        FileInputFormat.setInputPaths(job,new Path("D:\\data\\input\\join2"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\data\\output2"));

        // 将产品表缓存到task工作节点的工作目录中去
        job.addCacheFile(new URI("file:/D:/data/pdts.txt"));
        job.setNumReduceTasks(0);

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
