import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;


public class ChiSquareReducer extends org.apache.hadoop.mapreduce.Reducer<CategoryAKey, TokenABValue, Text, Text> {

    // todo: ask if this is bad prcatice to store te values in a data structure or if it is ok for only 150 values

    public void reduce(CategoryAKey key, Iterable<TokenABValue> values, Context context) throws IOException, InterruptedException {

        Top150ChiSquareTokens top150 = new Top150ChiSquareTokens();

        // todo: test if correctly sorted
        TokenABValue tokenAB = values.iterator().next();
        double categoryCount = tokenAB.getA().get();
        double n = categoryCount + tokenAB.getB().get();

        while (values.iterator().hasNext()) {
            tokenAB = values.iterator().next();
            double chiSquare = calculateChiSquare(tokenAB.getAasDouble(), tokenAB.getBasDouble(), categoryCount, n);
            top150.addToken(tokenAB.getToken().toString(), chiSquare);
        }

        LinkedHashMap<String, Double> tokenMap = top150.getTokenMap();
        for (String token : tokenMap.keySet()) {
            context.write(key.getCategory(), new Text(token + ":" + tokenMap.get(token)));
        }
    }

    private double calculateChiSquare(double a, double b, double categoryCount, double n) {
        double c = categoryCount - a;
        double d = n - (a + b + c);

        return (n * Math.pow((a*d - b*c),2.)) / ((a+b) * (a+c) * (b+d) * (c+d));
    }


    private class Top150ChiSquareTokens {

        TreeMap<Double, ArrayList<String>> tokenTree;
        LinkedHashMap<String, Double> tokenMap;
        int size;
        double minChiSquare = Double.MAX_VALUE;

        public Top150ChiSquareTokens() {
            tokenTree = new TreeMap<>();
            tokenMap = new LinkedHashMap<>();
            size = 0;
        }

        public LinkedHashMap<String, Double> getTokenMap() {
            for (Double key : tokenTree.keySet()) {
                ArrayList<String> values = tokenTree.get(key);
                for (String val : values) {
                    tokenMap.put(val, key);
                }
            }
            assert tokenMap.size() == 150;
            return tokenMap;
        }

        public void addToken(String token, Double chiSquare) {
            if (size < 150) {
                add(chiSquare, token);
                minChiSquare = tokenTree.firstKey();
            } else {
                if (chiSquare > minChiSquare) {
                    remove(minChiSquare);
                    add(chiSquare, token);
                    minChiSquare = tokenTree.firstKey();
                }
            }
        }

        private void add(Double key, String value) {
            if (tokenTree.containsKey(key)) {
                tokenTree.get(key).add(value);
            } else {
                ArrayList<String> newValue = new ArrayList<>();
                newValue.add(value);
                tokenTree.put(key, newValue);
            }
            size++;
        }

        private void remove(Double key) {
            ArrayList<String> values = tokenTree.get(key);
            size -= values.size();
            tokenTree.remove(key);
        }
    }
}