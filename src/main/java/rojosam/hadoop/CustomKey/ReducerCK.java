package rojosam.hadoop.CustomKey;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by eduardo on 9/20/15.
 */
public class ReducerCK extends Reducer<KeyWritableCK, IntWritable, Text, IntWritable>{

    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(KeyWritableCK key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable v: values){
           sum += v.get();
        }
        outKey.set(key.getWord() + "\t" + key.getFileName());
        outValue.set(sum);
        ctx.write(outKey, outValue);
    }

}
