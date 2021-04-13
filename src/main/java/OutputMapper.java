import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;


public class OutputMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Arrays.stream(value.toString().split("\\r?\\n")).forEach(line -> {
            try {
                String[] kv = value.toString().split("\t");
                context.write(new Text(kv[0]), new Text(kv[1]));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });


    }
}