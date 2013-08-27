package org.ttmudt.hadoop.btsmapreducer;

import com.sun.org.apache.xerces.internal.util.SynchronizedSymbolTable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: cloudera
 * Date: 8/25/13
 * Time: 2:24 AM
 * To change this template use File | Settings | File Templates.
 */
public class BTSDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job conf = new Job();
        conf.setJobName("BTSIdIMPSMR");

        FileInputFormat.addInputPath(conf,new Path("data"));
        FileOutputFormat.setOutputPath(conf,new Path("Out/Output_"+System.currentTimeMillis()));

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setJarByClass(BTSDriver.class);

        conf.setMapperClass(BTSIDIMSIMapper.class);
        conf.setReducerClass(BTSIDIMSIReducer.class);;

        System.exit(conf.waitForCompletion(true)?0:1);

    }
}
