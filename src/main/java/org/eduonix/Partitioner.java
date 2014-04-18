package org.eduonix;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;


/**
 * hadoop jar Partitioner /user/training/pre_process /user/training/output

 */
public class Partitioner {

    private static boolean devMode = true;
    private static String uniquePathId = ""+System.currentTimeMillis();


    public static void main(String[] args) throws IOException {

        JobConf conf = new JobConf(Partitioner.class);
        conf.setJobName("sortexample");

        Path input = new Path("./pre_process/partition");
        Path testOutput = new Path("post_process/"+uniquePathId+"/");
        Path partitionOutput = new Path("post_process/partition");


        if(devMode) {

            FileInputFormat.setInputPaths(conf, input);
            //FileOutputFormat.setOutputPath(conf, testOutput);
            FileOutputFormat.setOutputPath(conf, partitionOutput);

        } else   {

            FileInputFormat.setInputPaths(conf, new Path(args[0]));
            FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        }

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setNumReduceTasks(2);

        JobClient.runJob(conf);

    }
}
