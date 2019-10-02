package com.hs.bigdata.hadoopinaction.mr.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

    protected OrderGroupingComparator(){
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 要求只要id相同，就认为是相同的key

        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;

       return aBean.getOrderId().compareTo(bBean.getOrderId());

    }

}
