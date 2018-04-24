package bd.util;

import com.ibm.icu.text.BreakIterator;

public class Icu4j {

    public static String toAnalysis(String text){
        BreakIterator boundary = BreakIterator.getWordInstance();
        StringBuffer stringBuffer = new StringBuffer();

//        String sentence = "بعد التكوين وفقا لما سبق، يتم ذلك أساسا لجميع تكوينات الذاكرة.\n";
        boundary.setText(text);
        int start = boundary.first();
        for (int end = boundary.next(); end != BreakIterator.DONE; start = end, end = boundary.next()) {
            String word = text.substring(start, end);
            stringBuffer.append(word.trim());
            stringBuffer.append(",");
        }
        return stringBuffer.toString();
    }

//    public static void main(String[] args){
//        System.out.println("----------------CHINA----------------");
//        split(Locale.CHINA);
//    }
}
