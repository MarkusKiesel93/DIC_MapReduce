import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.LinkedHashMap;


public class ChiSquareReducer extends org.apache.hadoop.mapreduce.Reducer<CategoryTokenKey, TokenABValue, Text, Text> {

    private final static Text N_TOKEN = new Text("N");

    public void reduce(CategoryTokenKey key, Iterable<TokenABValue> values, Context context) throws IOException, InterruptedException {

        Top150ChiSquareTokens top150 = new Top150ChiSquareTokens();
        double n = 0.;
        double categoryCount = 0.;
        TokenABValue tokenAB;

        // first entry for category should always be N
        try {
            tokenAB = values.iterator().next();
            if (!tokenAB.getToken().equals(N_TOKEN)) {
                throw new Exception("N not sorted into first row");
            }
            categoryCount = tokenAB.getA().get();
            n = categoryCount + tokenAB.getB().get();
        } catch (Exception e) {
            System.out.println(e.toString());
        }

        // precess all further rows
        TokenABValue currentTokenAB = null;
        double a = 0.;
        double b = 0.;

        while (values.iterator().hasNext()) {
            tokenAB = values.iterator().next();
            if (currentTokenAB == null) {
                currentTokenAB = tokenAB;
            }
            if (currentTokenAB.equals(tokenAB)) {
                a += tokenAB.getAasDouble();
                b += tokenAB.getBasDouble();
            } else if (!currentTokenAB.equals(tokenAB)) {
                double chiSquare = calculateChiSquare(a, b, categoryCount, n);
                top150.add(currentTokenAB.getToken().toString(), chiSquare);

                currentTokenAB = tokenAB;
                a = tokenAB.getAasDouble();
                b = tokenAB.getBasDouble();
            }
        }

        StringBuilder out = new StringBuilder();
        LinkedHashMap<String, Double> tokenMap = top150.getTokenMap();
        for (String token : tokenMap.keySet()) {
            DecimalFormat df = new DecimalFormat("###.####");
            String chiSquare = df.format(tokenMap.get(token));
            out.append(token).append(":").append(chiSquare).append(" ");
        }
        context.write(key.getCategory(), new Text(out.toString()));
    }

    private double calculateChiSquare(double a, double b, double categoryCount, double n) {
        double c = categoryCount - a;
        double d = n - (a + b + c);

        return (n * Math.pow((a*d - b*c),2.)) / ((a+b) * (a+c) * (b+d) * (c+d));
    }


    private class Top150ChiSquareTokens {

        LinkedHashMap<String, Double> tokenMap;
        String cutoffToken;
        Double cutoffValue;

        public Top150ChiSquareTokens() {
            tokenMap = new LinkedHashMap<>();
            cutoffToken = null;
            cutoffValue = Double.MAX_VALUE;
        }

        public LinkedHashMap<String, Double> getTokenMap() {
            return tokenMap;
        }

        public void add(String token, Double value) {
            if (tokenMap.size() < 150) {
                tokenMap.put(token, value);
                if (value < cutoffValue) {
                    setNewCutoff(token, value);
                }
            } else {
                if (value > cutoffValue) {
                    tokenMap.remove(cutoffToken);
                    tokenMap.put(token, value);
                    setNewCutoff();
                }
            }
        }

        private void setNewCutoff(String token, Double value) {
            cutoffToken = token;
            cutoffValue = value;
        }

        private void setNewCutoff() {
            double min = Double.MAX_VALUE;
            String token = null;
            for (String t : tokenMap.keySet()) {
                double v = tokenMap.get(t);
                if (v < min) {
                    token = t;
                    min = v;
                }
            }
            cutoffToken = token;
            cutoffValue = min;
        }
    }
}