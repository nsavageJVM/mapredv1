package org.eduonix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.LineReader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.*;

/**
 *http://www.philippeadjiman.com/blog/2009/12/20/hadoop-tutorial-series-issue-2-getting-started-with-customized-partitioning/
 * hadoop jar /user/training/input /user/training/pre_process
 * https://developer.yahoo.com/hadoop/tutorial/module5.html
 * http://walrus.wr.usgs.gov/NAMSS/interactive_map.html
 * http://shrikantbang.wordpress.com/2013/10/22/hadoop-custom-input-format/
 * https://www.inkling.com/read/hadoop-definitive-guide-tom-white-3rd/chapter-7/input-formats

 */
public class PartitionGenerator {

    private static boolean devMode = true;
    private static String uniquePathId = ""+System.currentTimeMillis();

    static class PreprocessorMapper extends MapReduceBase implements Mapper {

        private Text word = new Text();


        @Override
        public void map(Object key, Object value, OutputCollector outputCollector, Reporter reporter) throws IOException {

            LongWritable longKey = (LongWritable)key;
            Text textvalue = (Text)value;
            int nbOccurences = 0;

            String line = textvalue.toString();
            String[] tokens = line.split(" ");
            if( tokens == null || tokens.length != 2 ){
                System.err.print("Passing header line with itext line: "+line+"n");
                return;
            }

            try {
                nbOccurences = calculateValue(tokens);
            } catch (RuntimeException e) {
                return;
            }
            outputCollector.collect(new IntWritable(nbOccurences), word );


        }

      private int  calculateValue(String[] tokens) {

          BigDecimal key = BigDecimal.valueOf(Math.abs(Double.valueOf(tokens[0])));
          BigDecimal value = BigDecimal.valueOf(Double.valueOf(tokens[1]));
          key = key.movePointRight(6);
          value = value.movePointRight(6);
          word.set(String.valueOf( value.intValue()));
          return key.intValue();
      }




    }

    public static void main(String[] args) throws IOException  {

        JobConf conf = new JobConf(PartitionGenerator.class);

        Path input = new Path("./testData");
        Path testOutput = new Path("pre_process/"+uniquePathId+"/");
        Path partitionOutput = new Path("pre_process/partition");

        if(devMode) {

          FileInputFormat.setInputPaths(conf, input);
         // FileOutputFormat.setOutputPath(conf, testOutput);
          FileOutputFormat.setOutputPath(conf, partitionOutput);

        } else   {

            FileInputFormat.setInputPaths(conf, new Path(args[0]));
            FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        }

  //      conf.setInputFormat(SeismicInputFormat.class);
        conf.setMapperClass(PreprocessorMapper.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setNumReduceTasks(0);
        /**
         * Sequence files are a basic file based data structure  TextOutputFormat.class)
         * persisting the key/value pairs in a binary format  SequenceFileOutputFormat
         */
        conf.setOutputFormat(TextOutputFormat.class);

        JobClient.runJob(conf);




    }


}
