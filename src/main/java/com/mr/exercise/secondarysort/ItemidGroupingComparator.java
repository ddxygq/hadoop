package com.mr.exercise.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * GroupingComparator是在reduce阶段分组来使用的
 *
 * Created by Administrator on 2018/3/6.
 */
public class ItemidGroupingComparator extends WritableComparator {
    //传入作为key的bean的class类型，以及制定需要让框架做反射获取实例对象
    protected ItemidGroupingComparator() {
        super(OrderBean.class, true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean abean = (OrderBean) a;
        OrderBean bbean = (OrderBean) b;

        //比较两个bean时，指定只比较bean中的orderid
        return abean.getItemid().compareTo(bbean.getItemid());
    }
}


// 参考博客：http://blog.csdn.net/qq_20641565/article/details/53491257
//           http://blog.csdn.net/qq_21050291/article/details/72871559
