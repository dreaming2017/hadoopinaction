package com.hs.bigdata.hadoopinaction.mr.sort2partioner;

import com.hs.bigdata.hadoopinaction.mr.sort.FlowBean;
import com.hs.bigdata.hadoopinaction.mr.sort.FlowCountSortMapper;
import com.hs.bigdata.hadoopinaction.mr.sort.FlowCountSortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountPartAndSortDriver {
    public static void main(String[] args) throws Exception {

        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "input/sort.txt", "output/sort2/" };
        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCountPartAndSortDriver.class);

        // 3 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);


        //4 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        //关联分区
        job.setPartitionerClass(ProvincePartitioner2.class);
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
