import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class ChiSquaredByCategory {

    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            throw new Exception("needs 4 arguments: inputPath, intermediatePath1, intermediatePath2, outputPath");
        }

        Path inputPath = new Path(args[0]);
        Path intermediatePath1 = new Path(args[1]);
        Path intermediatePath2 = new Path(args[2]);
        Path outputPath = new Path(args[3]);

        job1(inputPath, intermediatePath1);
        job2(intermediatePath1, intermediatePath2);
        job3(intermediatePath2, outputPath);
    }

    public static void job1(Path inputPath, Path outputPath) throws InterruptedException, IOException, ClassNotFoundException {
        // configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job 1");
        job.setJarByClass(ChiSquaredByCategory.class);

        // input / output
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // mapper
        // input: key -> Null, value -> (json object by line)
        // output: key -> TOKEN, value -> (CATEGORY, 1)
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CategoryCountValue.class);

        // combiner
        // input: key -> TOKEN, values -> [(CATEGORY, 1), ...]
        // output: key -> TOKEN, value -> (CATEGORY, COUNT)
        job.setCombinerClass(TokenCombiner.class);

        // reducer
        // input: key -> TOKEN, values -> [(CATEGORY, COUNT), ...]
        // output: key -> CATEGORY, value -> (TOKEN, A, B)
        job.setNumReduceTasks(30);
        job.setReducerClass(ABReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TokenABValue.class);

        // wait for job finished
        job.waitForCompletion(true);
    }

    public static void job2(Path inputPath, Path outputPath) throws InterruptedException, IOException, ClassNotFoundException {
        // configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job 2");
        job.setJarByClass(ChiSquaredByCategory.class);

        // input / output
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // mapper
        // input: key -> Null, value -> (CATEGORY, TOKEN, A, B split by line)
        // output: key -> (CATEGORY, A), value -> (TOKEN, A, B)
        job.setMapperClass(CategoryTokenABMapper.class);
        job.setMapOutputKeyClass(CategoryAKey.class);
        job.setMapOutputValueClass(TokenABValue.class);

        // partitioning and grouping
        // because of the composite key CategoryAKey (CATEGORY, A) we need a Partitioner and GroupingComparator
        // here we make sure that each reducer gets data for one CATEGORY
        // A is used for sorting because the highest value should appear first in the reducer ->
        // there the category count "N" is stored which is needed for computing the chi-squared values
        job.setPartitionerClass(CategoryAPartitioner.class);
        job.setGroupingComparatorClass(CategoryAComparator.class);

        // reducer
        // input: key -> (CATEGORY, A), values -> [(TOKEN, A, B), ...]
        // output: key -> CATEGORY, value -> (TOKEN, CHI-SQUARED)
        job.setNumReduceTasks(15);
        job.setReducerClass(ChiSquaredReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // wait for job finished
        job.waitForCompletion(true);
    }

    public static void job3(Path inputPath, Path outputPath) throws InterruptedException, IOException, ClassNotFoundException {
        // configuration
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf, "job 3");
        job.setJarByClass(ChiSquaredByCategory.class);

        // input / output
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // mapper
        // input: key -> Null, value -> (CATEGORY, TOKEN, CHI-SQUARED split by line)
        // output: key -> (CATEGORY OR T, CHI_SQUARED OR TOKEN), value -> TOKEN OR TOKEN:CHI-SQUARED
        job.setMapperClass(OutputMapper.class);
        job.setMapOutputKeyClass(OutputKey.class);
        job.setMapOutputValueClass(Text.class);

        // could already use a combiner to remove duplicates from tokens but improvement only minimal

        // partitioning and grouping
        // because of the composite key OutputKey (CATEGORY OR T, CHI_SQUARED OR TOKEN) we need a Partitioner and GroupingComparator
        // here we make sure that each reducer gets data for one CATEGORY or T (last line merged for all tokens)
        // CHI-SQUARED is used for sorting in each category descending
        // TOKEN is used for sorting the merged list of tokens alphabetically
        job.setPartitionerClass(OutputPartitioner.class);
        job.setGroupingComparatorClass(OutputComparator.class);

        // reducer
        job.setNumReduceTasks(2);
        job.setReducerClass(OutputReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // wait for job finished
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
