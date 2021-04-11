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

        String encodedCounts = job1(inputPath, intermediatePath);
//        job2(intermediatePath, outputPath, encodedCounts);

    }

    public static String job1(Path inputPath, Path outputPath) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job 1");
        job.setJarByClass(ChiSquareByCategory.class);

        // mapper
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CategoryCountValue.class);

        // combiner
        job.setCombinerClass(TokenCombiner.class);

//        // partition and grouping
//        job.setPartitionerClass(MyPartitioner.class);
//        job.setGroupingComparatorClass(MyComparator.class);

        // reducer
        job.setReducerClass(ABReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TokenABValue.class);

        // input / output
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // wait for job finished
        job.waitForCompletion(true);

        CounterGroup category = job.getCounters().getGroup("CATEGORY");
        StringBuilder encodedCounts = new StringBuilder();
        int n = 0;
        for (Counter counter : category) {
            long count = counter.getValue();
            n += count;
            encodedCounts.append(counter.getName()).append(":");
            encodedCounts.append(count).append(":");
        }
        encodedCounts.append("N").append(":").append(n);

        return encodedCounts.toString();
    }

    public static void job2(Path inputPath, Path outputPath, String encodedCounts) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] countsSplit = encodedCounts.split(":");
        for (int i = 0; i<countsSplit.length; i+=2) {
            conf.set(countsSplit[i], countsSplit[i+1]);
        }

        Job job = Job.getInstance(conf, "job 2");
        job.setJarByClass(ChiSquareByCategory.class);
        job.setMapperClass(MyMapper2.class);
        job.setReducerClass(MyReducer2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
