package rojosam.hadoop.PartitionerWordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

;

/**
 * Created by eduardo on 9/20/15.
 */
public class DriverPWC extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DriverPWC(), args);
        System.exit(res);
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Partitioner Word Count");
        job.setJarByClass(DriverPWC.class);
        job.setMapperClass(MapperPWC.class);
        job.setReducerClass(ReducerPWC.class);

        //Set the combiner
        job.setCombinerClass(ReducerPWC.class);
        job.setPartitionerClass(PartitionerPWC.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(4);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
