package rojosam.hadoop.PartitionerWordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by eduardo on 9/20/15.
 */
public class PartitionerPWC extends Partitioner<Text, IntWritable> {


    /**
     *
     * @param key
     * @param value
     * @param i
     * @return
     *
     * Split the mapper's output by the numeric value of the first character of the word.
     */

    @Override
    public int getPartition(Text key, IntWritable value, int i) {
        String word = key.toString();
        int firstChar = word.charAt(0);
        return ((firstChar-1) % i);
    }
}
