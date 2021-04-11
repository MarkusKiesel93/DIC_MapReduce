import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class ChiSquareByCategory {

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            throw new Exception("needs 3 arguments: inputPath, intermediatePath, outputPath");
        }

        Path inputPath = new Path(args[0]);
        Path intermediatePath = new Path(args[1]); // todo: find better way
        Path outputPath = new Path(args[2]);

        job1(inputPath, intermediatePath);
        job2(intermediatePath, outputPath);

    }

    public static void job1(Path inputPath, Path outputPath) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job 1");
        job.setJarByClass(ChiSquareByCategory.class);

        // mapper
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CategoryCountValue.class);

        // combiner
        job.setCombinerClass(TokenCombiner.class);

        // reducer
        job.setReducerClass(ABReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TokenABValue.class);

        // input / output
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // wait for job finished
        job.waitForCompletion(true);
    }

    public static void job2(Path inputPath, Path outputPath) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job 2");
        job.setJarByClass(ChiSquareByCategory.class);

        // mapper
        job.setMapperClass(CategoryTokenABMapper.class);
        job.setMapOutputKeyClass(CategoryTokenKey.class);
        job.setMapOutputValueClass(TokenABValue.class);

        // partition and grouping
        job.setPartitionerClass(CategoryTokenPartitioner.class);
        job.setGroupingComparatorClass(CategoryTokenComparator.class);

        // reducer
        job.setReducerClass(ChiSquareReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // input / output
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // wait for job finished
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
