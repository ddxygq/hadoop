package com.mr.exercise.secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Administrator on 2018/3/6.
 */
public class ItemIdPartitioner extends Partitioner<OrderBean,NullWritable>{

    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        return (orderBean.getItemid().hashCode() & Integer.MAX_VALUE) % i;
    }
}
