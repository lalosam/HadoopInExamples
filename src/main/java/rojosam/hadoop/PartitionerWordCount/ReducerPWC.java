package rojosam.hadoop.PartitionerWordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by eduardo on 9/20/15.
 */
public class ReducerPWC extends Reducer<Text, IntWritable, Text, IntWritable>{

    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable v: values){
           sum += v.get();
        }
        outKey.set(key);
        outValue.set(sum);
        ctx.write(outKey, outValue);
    }

}
