package rojosam.hadoop.CustomKey;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by eduardo on 9/21/15.
 */
public class CombinerCK extends Reducer<KeyWritableCK, IntWritable, KeyWritableCK, IntWritable> {

    private IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(KeyWritableCK key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable v: values){
            sum += v.get();
        }
        outValue.set(sum);
        ctx.write(key, outValue);
    }

}
