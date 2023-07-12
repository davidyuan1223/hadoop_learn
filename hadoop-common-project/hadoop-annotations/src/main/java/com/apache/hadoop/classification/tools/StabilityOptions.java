package com.apache.hadoop.classification.tools;

import com.sun.javadoc.DocErrorReporter;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

class StabilityOptions {
    public static final String STABLE_OPTION="-stable";
    public static final String EVOLVING_OPTION="-evolving";
    public static final String UNSTABLE_OPTION="-unstable";

    public static Integer optionLength(String option){
        String opt = option.toLowerCase(Locale.ENGLISH);
        if (opt.equals(UNSTABLE_OPTION)) return 1;
        if (opt.equals(EVOLVING_OPTION)) return 1;
        if (opt.equals(STABLE_OPTION)) return 1;
        return null;
    }
    public static void validOptions(String[][] options, DocErrorReporter reporter){
        for (String[] option : options) {
            String opt = option[0].toLowerCase(Locale.ENGLISH);
            switch (opt) {
                case UNSTABLE_OPTION:
                    RootDocProcessor.stability = UNSTABLE_OPTION;
                    break;
                case EVOLVING_OPTION:
                    RootDocProcessor.stability = EVOLVING_OPTION;
                    break;
                case STABLE_OPTION:
                    RootDocProcessor.stability = STABLE_OPTION;
                    break;
            }
        }
    }
    public static String[][] filterOptions(String[][] options){
        List<String[]> optionList=new ArrayList<>();
        for (String[] option : options) {
            if (!option[0].equalsIgnoreCase(UNSTABLE_OPTION)
            &&!option[0].equalsIgnoreCase(EVOLVING_OPTION)
            &&!option[0].equalsIgnoreCase(STABLE_OPTION)){
                optionList.add(option);
            }
        }
        String[][] filterOptions=new String[optionList.size()][];
        int i=0;
        for (String[] option : optionList) {
            filterOptions[i++]=option;
        }
        return filterOptions;
    }
}
