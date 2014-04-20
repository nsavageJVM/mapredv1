package org.eduonix.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.eduonix.ETLRunner;
import java.io.IOException;
import java.math.BigDecimal;


public class SeismicETL {



    Path input;
    Path output;
    Path outputHDFS = new Path("./tmp/sesmicData");

    public SeismicETL(Path input, Path output) {
        this.input = input;
        this.output = output;
    }



    public void extract() throws IOException, ClassNotFoundException, InterruptedException {

        JobConf conf = new JobConf(ETLRunner.class);
        FileSystem.get(output.toUri(),conf).delete(output, true);
        FileInputFormat.setInputPaths(conf, input);
        FileOutputFormat.setOutputPath(conf, output);
        conf.setMapperClass(LoadMapper.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setNumReduceTasks(0);
        conf.setOutputFormat(TextOutputFormat.class);
        JobClient.runJob(conf);



    }



    public void transform(Path transformOutput) throws IOException, InterruptedException {
        JobConf conf = new JobConf(ETLRunner.class);
        FileSystem.get(transformOutput.toUri(),conf).delete(transformOutput, true);
        Path pinput = new Path(output, "part-00000");
        System.out.println(pinput.toString());
        FileInputFormat.setInputPaths(conf, pinput);
        FileOutputFormat.setOutputPath(conf, transformOutput);
        conf.setMapperClass(TransformMapper.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setNumReduceTasks(0);
        /**
         * Sequence files are a basic file based data structure
         * persisting the key/value pairs in a binary format SequenceFileInputFormat
         * conf.setOutputFormat(SequenceFileOutputFormat.class);
         */
        conf.setOutputFormat(TextOutputFormat.class);

        JobClient.runJob(conf);

    }


    public void load(Path transformDataOutput) throws IOException {

        FileSystem fs = FileSystem.getLocal(new Configuration());
        fs.delete(outputHDFS, true);
        fs.mkdirs(outputHDFS);
        // use clusterPath in place of transformDataOutput in
        //  fs.copyToLocalFile( transformDataOutput, outputHDFS );for hortonworks cluster
        Path clusterPath = new Path("/user/root/EtlDataOut");
        fs.copyToLocalFile( clusterPath, outputHDFS );
        FileStatus[] filList = fs.listStatus(outputHDFS);
        // print out all the files.
        for (FileStatus stat : filList) {
            System.out.println(stat.getPath() + "  " + stat.getLen());
        }
        System.out.println();

    }




    static class LoadMapper extends MapReduceBase implements Mapper {

        private Text keyWord = new Text();
        private Text valWord = new Text();

        @Override
        public void map(Object key, Object value, OutputCollector outputCollector, Reporter reporter) throws IOException {

            Text textValue = (Text)value;
            String line = textValue.toString();
            String[] tokens = line.split(" ");

            if( tokens == null || tokens.length != 2 ){
              //  System.err.print("Passing header line with itext line: "+line+"n");
                return;
            }

            try {
               clean(tokens);
            } catch (RuntimeException e) {
                return;
            }
            outputCollector.collect(keyWord, valWord );


        }

        private void  clean(String[] tokens) {

            BigDecimal key = BigDecimal.valueOf(Double.valueOf(tokens[0]));
            BigDecimal value = BigDecimal.valueOf(Double.valueOf(tokens[1]));
            keyWord.set(String.valueOf( key.doubleValue()));
            valWord.set(String.valueOf( value.doubleValue()));
        }


    }


    static class TransformMapper extends MapReduceBase implements Mapper {

        private Text keyWord = new Text();
        private Text valWord = new Text();

        @Override
        public void map(Object key, Object value, OutputCollector outputCollector, Reporter reporter) throws IOException {

            Text textValue = (Text)value;
            String line = textValue.toString();
            String[] tokens = line.split("\t");
            BigDecimal k = BigDecimal.valueOf(Double.valueOf(tokens[0]));
            k = k.movePointRight(6);
            keyWord.set(String.valueOf( k.intValue()));
            BigDecimal v = BigDecimal.valueOf(Double.valueOf(tokens[1]));
            v = v.movePointRight(6);
            valWord.set(String.valueOf( v.intValue()));

            outputCollector.collect(keyWord, valWord );

        }

    }



}
