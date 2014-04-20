package org.eduonix;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.eduonix.etl.SeismicETL;

import java.io.IOException;

/**
 * Created by ubu on 4/19/14.
 */
public class ETLRunner {

    private static String uniquePathId = ""+System.currentTimeMillis();

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Path dataInput = new Path("./EtlDataInput");
        Path extractDataTestOutput = new Path("EtlDataOut/"+uniquePathId+"/extract");
        Path extractDataOutput = new Path("EtlDataOut/extract");
        Path transformDataTestOutput = new Path("EtlDataOut/"+uniquePathId+"/transform");
        Path transformDataOutput = new Path("EtlDataOut/transform");

        SeismicETL etl = new SeismicETL(dataInput, extractDataOutput);

        etl.extract();
        etl.transform(transformDataOutput);
        etl.load(transformDataOutput);

    }
}
