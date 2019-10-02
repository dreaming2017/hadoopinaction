package com.hs.bigdata.hadoopinaction.mr.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 每一行按着指定字符切分成 k v
 * banzhang ni hao   < banzhang,1>
 */
public class KVTextMapper extends Mapper<Text,Text,Text, IntWritable> {
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
       context.write(key,v);
    }
}
