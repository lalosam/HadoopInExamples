package rojosam.hadoop.CustomKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

;

/**
 * Created by eduardo on 9/20/15.
 */
public class DriverCK extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DriverCK(), args);
        System.exit(res);
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set("mapreduce.input.fileinputformat.split.maxsize", Long.toString(128 * 1024 * 1024));

        Job job = Job.getInstance(conf, "Combined Input & Partitioner Word Count");
        job.setJarByClass(DriverCK.class);
        job.setMapperClass(MapperCK.class);
        job.setReducerClass(ReducerCK.class);

        job.setPartitionerClass(PartitionerCK.class);
        job.setCombinerClass(CombinerCK.class);

        job.setMapOutputKeyClass(KeyWritableCK.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.addInputPath(job, new Path(args[0]));


        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(4);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
