package org.ttumdt.hadoop.btsmapreducer.generic;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.ttumdt.hadoop.btsmapreducer.generic.mr.generic.GenericBTSIDIMSIMapper;
import org.ttumdt.hadoop.btsmapreducer.generic.mr.generic.GenericBTSIDIMSIReducer;

import java.io.IOException;


public class GenericBTSDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job conf = new Job();
        conf.setJobName("BTSIdIMPSMR");

        FileInputFormat.addInputPath(conf,new Path("data"));
        FileOutputFormat.setOutputPath(conf,new Path("Out/Output_"+System.currentTimeMillis()));

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setJarByClass(GenericBTSDriver.class);

        conf.setMapperClass(GenericBTSIDIMSIMapper.class);
        conf.setReducerClass(GenericBTSIDIMSIReducer.class);
        conf.setOutputFormatClass(MultiTableOutputFormat.class);

        System.exit(conf.waitForCompletion(true)?0:1);

    }
}

