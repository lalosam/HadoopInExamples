package rojosam.hadoop.CombinedInputWordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

;

/**
 * Created by eduardo on 9/20/15.
 */
public class DriverCIPWC extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DriverCIPWC(), args);
        System.exit(res);
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //*********************************
        // It is important to set the maximum size of each combined split (in bytes)
        // In this case, I'm using the hadoop's block size to keep it similar than the
        // job with only one big file using the PartitionerWordCount job
        conf.set("mapreduce.input.fileinputformat.split.maxsize", Long.toString(128 * 1024 * 1024));

        Job job = Job.getInstance(conf, "Combined Input & Partitioner Word Count");
        job.setJarByClass(DriverCIPWC.class);
        job.setMapperClass(MapperCIPWC.class);
        job.setReducerClass(ReducerCIPWC.class);

        job.setCombinerClass(ReducerCIPWC.class);
        job.setPartitionerClass(PartitionerCIWC.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //*********************************
        // Adding the inputs CombineTextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.addInputPath(job, new Path(args[0]));


        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(4);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
