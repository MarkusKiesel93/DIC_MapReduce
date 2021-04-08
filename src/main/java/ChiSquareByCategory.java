import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kerby.config.Conf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class ChiSquareByCategory {

    public static void main(String[] args) throws Exception {

        // todo test for args
        Path inputPath = new Path(args[0]);
        Path intermediatePath = new Path(args[1]); // todo: find better way
        Path outputPath = new Path(args[2]);

        String encodedCounts = job1(inputPath, intermediatePath);
        job2(intermediatePath, outputPath, encodedCounts);



//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "word count");
//        job.setJarByClass(ChiSquareByCategory.class);
//        job.setMapperClass(TokenMapper.class);
//        job.setReducerClass(TokenReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static String job1(Path inputPath, Path outputPath) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job 1");
        job.setJarByClass(ChiSquareByCategory.class);
        job.setMapperClass(MyMapper1.class);
        job.setReducerClass(MyReducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
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
