package org.eduonix;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.*;


import java.io.IOException;
import java.util.Iterator;


/**
 *
 */
public class PartitionerPhase {

    private static boolean devMode = true;
    private static String uniquePathId = ""+System.currentTimeMillis();


    public static void main(String[] args) throws IOException {

        JobConf conf = new JobConf(PartitionerPhase.class);
        conf.setJobName("sortexample");

        Path input = new Path("./pre_process/partition");
        Path testOutput = new Path("post_process/"+uniquePathId+"/");
        // we will consider sorted data to be finished and ready to be reduced
        Path partitionOutput = new Path("post_process/partition");


        if(devMode) {

            FileInputFormat.setInputPaths(conf, input);
          //  FileOutputFormat.setOutputPath(conf, testOutput);
            FileOutputFormat.setOutputPath(conf, partitionOutput);

        } else   {

            FileInputFormat.setInputPaths(conf, new Path(args[0]));
            FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        }

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setNumReduceTasks(0);

        JobClient.runJob(conf);

    }

}


