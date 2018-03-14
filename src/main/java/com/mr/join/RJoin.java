package com.mr.join;

import com.bean.InfoBean;
import com.mr.wc.WordCountDriver;
import com.mr.wc.WordCountMapper;
import com.mr.wc.WordCountReducer;
import org.apache.commons.beanutils.BeanUtils;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * Created by Administrator on 2018/3/6.
 */
public class RJoin {
    static class RJoinMapper extends Mapper<LongWritable,Text,Text,InfoBean>{
        InfoBean bean = new InfoBean();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            FileSplit inputSplit = (FileSplit)context.getInputSplit();
            String name = inputSplit.getPath().getName();
            String p_id = "";

            // 通过文件名判断是哪种数据
            if(name.startsWith("order")){
                String[] fields = line.split("\t");
                p_id = fields[2];
                bean.set(Integer.parseInt(fields[0]),fields[1],p_id,Integer.parseInt(fields[3]),"",0,0,"0");
            }else {
                String[] fields = line.split("\t");
                p_id = fields[0];
                bean.set(0,"",p_id,0,fields[1],Integer.parseInt(fields[2]),Float.parseFloat(fields[3]),"1");
            }

            k.set(p_id);
            context.write(k,bean);
        }
    }

    static class RJoinReducer extends Reducer<Text,InfoBean,InfoBean,NullWritable>{
        @Override
        protected void reduce(Text pid, Iterable<InfoBean> beans, Context context) throws IOException, InterruptedException {
            InfoBean pdBean = new InfoBean(); // 产品bean，对于多条order bean
            ArrayList<InfoBean> orderBeans = new ArrayList<InfoBean>();
            for (InfoBean bean : beans){
                if("1".equals(bean.getFlag())){ // 产品的
                    try {
                        BeanUtils.copyProperties(pdBean,bean);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }else {
                    InfoBean odBean = new InfoBean();
                    try {
                        BeanUtils.copyProperties(odBean,bean);
                        orderBeans.add(odBean);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
            }

            for (InfoBean bean : orderBeans){
                bean.setPname(pdBean.getPname());
                bean.setCategory_id(pdBean.getCategory_id());
                bean.setPrice(pdBean.getPrice());
                context.write(bean,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://master1.hadoop:9000");

        Job job = Job.getInstance(conf);

        // 指定本程序的jar包所在的本地路径
        // job.setJarByClass(RJoin.class);
        job.setJar("D:\\IJ\\hadoop\\target\\RJoin.jar");

        // 指定本业务job使用的mapper/reducer作业
        job.setMapperClass(RJoinMapper.class);
        job.setReducerClass(RJoinReducer.class);

        // 指定mapper输出数据的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);

        // Combiner 不能影响最后的结果，比如平均值时候
        // job.setCombinerClass(WordCountReducer.class);

        // 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));

        // 指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
