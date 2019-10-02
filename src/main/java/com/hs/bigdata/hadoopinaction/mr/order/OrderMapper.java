package com.hs.bigdata.hadoopinaction.mr.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {

    OrderBean k = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        k.setOrderId(fields[0]);
        k.setProductId(fields[1]);
        k.setPrice(Double.parseDouble(fields[2]));
        context.write(k,NullWritable.get());
    }
}
