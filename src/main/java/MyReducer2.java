import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

public class MyReducer2 extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        double countCategory = Double.parseDouble(conf.get(key.toString()));
        double n = Double.parseDouble(conf.get("N"));

        HashMap<String, Double> tokenMapAll = new HashMap<>();
        for (Text val : values) {
            String[] valSplit = val.toString().split(":");
            String token = valSplit[0];
            double a = Double.parseDouble(valSplit[1]);
            double b = Double.parseDouble(valSplit[2]);
            double c = countCategory - a;
            double d = n - (a + b + c);
            double chi_square = (n * Math.pow((a*d - b*c),2.)) / ((a+b) * (a+c) * (b+d) * (c+d));
            tokenMapAll.put(token, chi_square);
        }

        // get threshold of top 150 values
        // todo: pick all if not 150 vals exist
        ArrayList<Double> mapValues = new ArrayList<>(tokenMapAll.values());
        Collections.sort(mapValues, Collections.reverseOrder());
        double minThreshold = mapValues.get(150);

        // sort alphabetically
        ArrayList<String> mapKeys = new ArrayList<>(tokenMapAll.keySet());
        Collections.sort(mapKeys);

        // get all values above threshold
        LinkedHashMap<String, Double> tokenMap = new LinkedHashMap<>();
        for (String token : mapKeys) {
            double chi_square = tokenMapAll.get(token);
            if (chi_square > minThreshold) {
                tokenMap.put(token, chi_square);
            }
        }

        StringBuilder out = new StringBuilder();
        for (String token : tokenMap.keySet()) {
            DecimalFormat df = new DecimalFormat("###.####");
            String chi_square = df.format(tokenMap.get(token));
            out.append(token).append(":").append(chi_square).append(" ");
        }
        context.write(key, new Text(out.toString()));
    }
}