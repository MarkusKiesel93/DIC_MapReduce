import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;


public class ChiSquaredReducer extends org.apache.hadoop.mapreduce.Reducer<CategoryAKey, TokenABValue, Text, Text> {

    public void reduce(CategoryAKey key, Iterable<TokenABValue> values, Context context) throws IOException, InterruptedException {

        // create data structure to efficiently store top 150 chi-squared values
        Top150ChiSquareTokens top150 = new Top150ChiSquareTokens();

        // each reducer is sorted so the highest value has to be the total number the category exists
        // the total count n is the number of times the category exists (A) + number of times the category does not exist (B)
        TokenABValue tokenAB = values.iterator().next();
        double categoryCount = tokenAB.getA().get();
        double n = categoryCount + tokenAB.getB().get();

        // iterate over all other values and calculating chi-square by token
        // each token only exists once for the category
        while (values.iterator().hasNext()) {
            tokenAB = values.iterator().next();
            double a = tokenAB.getAasDouble();
            double b = tokenAB.getBasDouble();
            // C -> number of times the document has the category but not the token
            double c = categoryCount - a;
            // D -> number of times the document does not have the category and token
            double d = n - (a + b + c);
            // calculate chi-squared according to given formula
            double chiSquare = (n * Math.pow((a*d - b*c),2.)) / ((a+b) * (a+c) * (b+d) * (c+d));

            // stores token if larger than the smallest current chi-squared value
            top150.addToken(tokenAB.getToken().toString(), chiSquare);
        }

        // emit the top 150 chi-squared values + tokens
        // key -> CATEGORY, value -> (TOKEN, CHI-SQUARED)
        HashMap<String, Double> tokenMap = top150.getTokenMap();
        for (String token : tokenMap.keySet()) {
            context.write(key.getCategory(), new Text(token + ":" + tokenMap.get(token)));
        }
    }


    // data structure used to store the top 150 chi-squared values
    private class Top150ChiSquareTokens {

        TreeMap<Double, ArrayList<String>> tokenTree;
        HashMap<String, Double> tokenMap;
        int size;
        double minChiSquare = Double.MAX_VALUE;

        public Top150ChiSquareTokens() {
            tokenTree = new TreeMap<>();
            tokenMap = new LinkedHashMap<>();
            size = 0;
        }

        // return the top values in a Map
        public HashMap<String, Double> getTokenMap() {
            for (Double key : tokenTree.keySet()) {
                ArrayList<String> values = tokenTree.get(key);
                for (String val : values) {
                    tokenMap.put(val, key);
                }
            }
            assert tokenMap.size() == 150;
            return tokenMap;
        }

        // add token and chi square value to TreeMap if chi-squared value larger than the smallest current value
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

        // add new token to TreeMap
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

        // remove token form TreeMap
        private void remove(Double key) {
            ArrayList<String> values = tokenTree.get(key);
            int len = values.size();
            if (len == 1) {
                tokenTree.remove(key);
            } else {
                tokenTree.get(key).remove(len - 1);
            }
            size--;
        }
    }
}