package com.hs.bigdata.hadoopinaction.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text key, FlowBean flowBean, int i) {
        // 获取手机号前三位
        String prePhoneNum = key.toString().substring(0, 3);
        switch (prePhoneNum) {
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }
}
