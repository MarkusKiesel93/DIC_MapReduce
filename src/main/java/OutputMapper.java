import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;


public class OutputMapper extends Mapper<Object, Text, OutputKey, Text> {

    private final static Text TOKEN = new Text("T");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Arrays.stream(value.toString().split("\\r?\\n")).forEach(line -> {
            try {
                String[] kv = value.toString().split("\t");
                Text category = new Text(kv[0]);
                String tokenValue = kv[1];
                Text tv = new Text(tokenValue);
                Text t = new Text(tokenValue.split(":")[0]);
                Text v = new Text(tokenValue.split(":")[1]);

                // map the category + token:chi_square
                OutputKey outKey1 = new OutputKey(category, v);
                context.write(outKey1, tv);

                // map TOKEN(special category for mapping all tokens) + token
                OutputKey outKey2 = new OutputKey(TOKEN, t);
                context.write(outKey2, t);

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });


    }
}