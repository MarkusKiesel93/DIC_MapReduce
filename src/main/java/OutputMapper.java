import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;


public class OutputMapper extends Mapper<Object, Text, OutputKey, Text> {

    private final static Text CATEGORY_TOKEN = new Text("T");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // stream over all lines in one file
        // value.toString() -> whole file -> split by new line
        Arrays.stream(value.toString().split("\\r?\\n")).forEach(line -> {
            try {
                // extract CATEGORY, TOKEN, CHI-SQUARED
                String[] kv = value.toString().split("\t");  // CATEGORY split by tab by default
                Text category = new Text(kv[0]);
                String tokenValue = kv[1];
                Text tv = new Text(tokenValue);
                Text t = new Text(tokenValue.split(":")[0]);  // Text was split by ":" TOKEN : CHI-SQUARED
                Text v = new Text(tokenValue.split(":")[1]);

                // emit: key -> (CATEGORY, CHI-SQUARED), value -> (TOKEN, CHI-SQUARED)
                // use composite key to sort by CHI-SQUARED value for the output
                OutputKey outKey1 = new OutputKey(category, v);
                context.write(outKey1, tv);

                // emit: key -> (CATEGORY_TOKEN, TOKEN), value -> TOKEN
                // use composite key to sort the tokens alphabetically
                OutputKey outKey2 = new OutputKey(CATEGORY_TOKEN, t);
                context.write(outKey2, t);

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });


    }
}