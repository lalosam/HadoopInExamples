package rojosam.utils;

/**
 * Created by eduardo on 9/20/15.
 */
public class NumberUtils {

    public static String padNum(int value, int maxValue){
        double d = Math.log10(maxValue);
        int maxDigits = (int)Math.log10(maxValue);
        String strValue = Integer.toString(value);
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i <= (maxDigits - strValue.length()); i++){
            sb.append("0");
        }
        sb.append(strValue);
        return sb.toString();
    }
}
