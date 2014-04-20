package org.eduonix.udf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import java.util.Iterator;



/**
 * Created by ubu on 4/20/14.
 */
public class PigUDF extends EvalFunc<String> {

    PigServer pigServer;

    private static String jatPath ="/home/ubu/MapreduceVersion1/target/MapReduceModule-jar-with-dependencies.jar";

    public static void main(String[] args) throws IOException {

        PigUDF pigUDF = new PigUDF (new PigServer(ExecType.LOCAL));
        pigUDF.runQuery();

    }

    public PigUDF() {
    }

    public PigUDF(PigServer pigServer) throws IOException {

        Path path = new Path("./tmp/sesmicData/pig");
        FileSystem.get(path.toUri(), new Configuration()).delete(path, true);

        this.pigServer = pigServer;

        try {
            pigServer.registerJar(jatPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

   public void runQuery() throws IOException {

       pigServer.registerQuery(  "pig_udf = LOAD './tmp/sesmicData/transform/part-00000';");
       pigServer.registerQuery(  "pig_gen = foreach pig_udf generate $0 as x, $1 as y;");

       System.out.println("Schema : " + pigServer.dumpSchema("pig_udf"));

       Iterator<Tuple> tuples = pigServer.openIterator("pig_udf");

       while(tuples.hasNext()){
           Tuple t = tuples.next();
           System.out.println((t.get(0) + " " + t.get(1)));
       }


       pigServer.registerQuery(  "pig_result = foreach pig_gen  GENERATE org.eduonix.udf.PigUDF(x,y);");



       pigServer.store("pig_result", "./tmp/sesmicData/pig");
   }




    @Override
    public String exec(Tuple input) throws IOException {

        if (null == input || input.size() == 0) {
            return "found null";
        }

        DataByteArray key = (DataByteArray)input.get(0);
        DataByteArray value = (DataByteArray)input.get(1);
        String aggregated_result = "pig processed output: key: "+key.toString()+"  value: "+value.toString();

        return aggregated_result;
    }




}
