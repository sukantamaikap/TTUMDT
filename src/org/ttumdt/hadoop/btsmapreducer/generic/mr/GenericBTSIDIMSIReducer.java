package org.ttumdt.hadoop.btsmapreducer.generic.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: cloudera
 * Date: 8/15/13
 * Time: 3:05 AM
 * To change this template use File | Settings | File Templates.
 */
public class GenericBTSIDIMSIReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce (Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<String> entries = new ArrayList<String>();
        for (Text value : values) {
            // ToDo : more error handling code to come here
            //if(value.getLength() == 24){
                entries.add(value.toString());
            //}
        }
        context.write(key,new Text(entries.toString()));
    }
}
