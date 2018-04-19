package bd.util;

import bd.constant.Constant;
import com.spreada.utils.chinese.ZHConverter;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.recognition.impl.FilterRecognition;
import org.ansj.recognition.impl.SynonymsRecgnition;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.nlpcn.commons.lang.tire.domain.Forest;
import org.nlpcn.commons.lang.tire.library.Library;

import java.util.List;

public class ANSJUtil {
    private static ANSJUtil instance = new ANSJUtil();
    private ZHConverter converter;
    private FilterRecognition filter;
    private Forest forest;
    private SynonymsRecgnition synonymsRecgnition;
    private ANSJUtil(){
        converter = ZHConverter.getInstance(ZHConverter.SIMPLIFIED);
        filter = new FilterRecognition();
        filter.insertStopNatures("en","null","m","e","u","y","o","w");
        filter.insertStopWords(Constant.stopWords);
        forest = Library.makeForest(Constant.values);
        synonymsRecgnition = new SynonymsRecgnition();
        for (String[] synonym:Constant.synonyms) {
            synonymsRecgnition.insert(synonym);
        }

    }
    public static ANSJUtil getInstance(){
        return instance;
    }

    public String toAnalysis(String text) throws Exception {
        StringBuffer stringBuffer = new StringBuffer();
        text = converter.convert(text).toLowerCase();

//        Result simple
        Result result = ToAnalysis.parse(text,forest).recognition(filter).recognition(synonymsRecgnition); //分词结果的一个封装，主要是一个List<Term>的terms
//        Result result = ToAnalysis.parse(text);
//        System.out.println(result.getTerms());

        List<Term> terms = result.getTerms(); //拿到terms
//        System.out.println(terms.size());

        for(int i=0; i<terms.size(); i++) {
            String word = terms.get(i).getName().trim(); //拿到词
            String natureStr = terms.get(i).getNatureStr(); //拿到词性
            List<String> synonyms = terms.get(i).getSynonyms();

            stringBuffer.append(word);
            stringBuffer.append("\t");
            stringBuffer.append(natureStr);
            stringBuffer.append("\t");
            if(synonyms != null){
                stringBuffer.append(synonyms.get(0));
            }else {
                stringBuffer.append(word);
            }
            stringBuffer.append(",");
        }

        return stringBuffer.toString();
    }
}
