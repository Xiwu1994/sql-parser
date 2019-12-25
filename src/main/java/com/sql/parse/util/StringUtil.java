package com.sql.parse.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {

    public static String subVariable(String str) {
        /**
         * ${yesterday}  ->  'yesterday'
         */
        Pattern leftpattern = Pattern.compile("\\$\\{");
        Matcher leftmatcher = leftpattern.matcher(str);
        Pattern rightpattern = Pattern.compile("\\}");
        Matcher rightmatcher = rightpattern.matcher(str);
        int begin = 0;
        List<String> foundKeys = new ArrayList<>();
        while (leftmatcher.find(begin)) {
            rightmatcher.find(leftmatcher.start());
            String configKey = str.substring(leftmatcher.start(), rightmatcher.end());
            foundKeys.add(configKey);
            begin = rightmatcher.end();
        }
        for (String foundkey : foundKeys){
            str = str.replace(foundkey, foundkey.replace("${","'").replace("}", "'"));
        }
        return str;
    }
}
