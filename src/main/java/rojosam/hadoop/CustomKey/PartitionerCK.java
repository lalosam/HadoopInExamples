package rojosam.hadoop.CustomKey;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by eduardo on 9/20/15.
 */
public class PartitionerCK extends Partitioner<KeyWritableCK, IntWritable> {


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
    public int getPartition(KeyWritableCK key, IntWritable value, int i) {
        String word = key.getWord();
        int firstChar = word.charAt(0);
        return ((firstChar-1) % i);
    }
}
