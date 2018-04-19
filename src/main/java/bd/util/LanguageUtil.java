package bd.util;

import com.spreada.utils.chinese.ZHConverter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LanguageUtil {

    /**
     * 判断文本中是否包含中文
     * @param text 输入文本
     * @return true:包含中文 false:不包含中文
     */
    public static boolean containChinese(String text){
        String regEx = "[\\u4E00-\\u9FA5]+";
        Pattern p = Pattern.compile(regEx);
        ZHConverter converter = ZHConverter.getInstance(ZHConverter.SIMPLIFIED);
        Matcher m = p.matcher(converter.convert(text));
        if (m.find()) {
            return true;
        } else {
            return false;
        }
    }
}
