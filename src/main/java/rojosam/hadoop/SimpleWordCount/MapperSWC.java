package rojosam.hadoop.SimpleWordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by eduardo on 9/20/15.
 */
public class MapperSWC extends Mapper<LongWritable, Text, Text, IntWritable>{

    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
        for(String s:words){
            outKey.set(s);
            ctx.write(outKey,outValue);
        }
    }
}
