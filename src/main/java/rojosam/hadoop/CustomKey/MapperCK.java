package rojosam.hadoop.CustomKey;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by eduardo on 9/20/15.
 */
public class MapperCK extends Mapper<LongWritable, Text, KeyWritableCK, IntWritable>{

    private KeyWritableCK outKey = new KeyWritableCK();
    private IntWritable outValue = new IntWritable(1);
    private String filename = null;

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        try {
            filename = new Path(new URI(ctx.getConfiguration().get(MRJobConfig.MAP_INPUT_FILE))).getName();
        } catch (URISyntaxException use) {
            throw new RuntimeException("The path of the file being processed is malformed: "
                    + ctx.getConfiguration().get(MRJobConfig.MAP_INPUT_FILE));
        }
        String line = value.toString();
        String[] words = line.split("\t");
        for(String s:words){
            outKey.setFileName(filename);
            outKey.setWord(s);
            ctx.write(outKey,outValue);
        }
    }
}
