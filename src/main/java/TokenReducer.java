import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

public class TokenReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // extract all tokens from all text iterables
        ArrayList<String> tokensList = new ArrayList<>();
        for (Text val : values) {
            String[] items = val.toString().split(":");
            for (String item : items) {
                tokensList.add(item);
            }
        }
        // sort the list to get data into alphabetical order
        Collections.sort(tokensList);

        // get counts per token
        LinkedHashMap<String, Integer> tokensMapAll = new LinkedHashMap<>();
        for (String token : tokensList) {
            tokensMapAll.merge(token, 1, Integer::sum);
        }

        // get threshold of top 150 values
        // todo: pick all if not 150 vals exist
        ArrayList<Integer> mapValues = new ArrayList<>(tokensMapAll.values());
        Collections.sort(mapValues, Collections.reverseOrder());
        int minThreshold = mapValues.get(150);

        // get all values above threshold
        LinkedHashMap<String, Integer> tokensMap = new LinkedHashMap<>();
        for (String token : tokensMapAll.keySet()) {
            int tokenCount = tokensMapAll.get(token);
            if (tokenCount > minThreshold) {
                tokensMap.put(token, tokenCount);
            }
        }

        // create output
        StringBuilder out = new StringBuilder();
        for (String token : tokensMap.keySet()) {
            out.append(token).append(":").append(tokensMap.get(token)).append(" ");
        }

        context.write(key, new Text(out.toString()));
    }
}