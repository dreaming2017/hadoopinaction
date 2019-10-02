package com.hs.bigdata.hadoopinaction.mr.sort2partioner;

import com.hs.bigdata.hadoopinaction.mr.sort.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner2 extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        // 获取手机号前三位
        String prePhoneNum = text.toString().substring(0, 3);
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
