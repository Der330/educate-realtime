package com.atguigu.edu.realtime.dws.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: 刘大大
 * @CreateTime: 2024/9/13  13:55
 */


public class KeywordUtil {
    //分词
    public static List<String> analyze(String text){
        List<String> keywordList = new ArrayList<>();
        StringReader reader = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(reader,true);
        try {
            Lexeme lexeme = null;
            while ((lexeme = ik.next())!=null){
                String keyword = lexeme.getLexemeText();
                keywordList.add(keyword);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return keywordList;
    }

   /* public static void main(String[] args) {
        System.out.println(analyze("凯哥凯哥 五万五万"));
    }*/
}
