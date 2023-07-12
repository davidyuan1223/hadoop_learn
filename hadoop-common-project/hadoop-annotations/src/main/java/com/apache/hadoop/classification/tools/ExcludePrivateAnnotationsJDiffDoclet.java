package com.apache.hadoop.classification.tools;

import com.sun.javadoc.DocErrorReporter;
import com.sun.javadoc.LanguageVersion;
import com.sun.javadoc.RootDoc;
import jdiff.JDiff;

/**
 * A Doclet for excluding elements that are annotated with Private or LimitedPrivate
 * It delegates to the JDiff Doclet,and takes the same options
 */
public class ExcludePrivateAnnotationsJDiffDoclet {
    public static LanguageVersion languageVersion(){return LanguageVersion.JAVA_1_5;}
    public static boolean start(RootDoc doc){
        System.out.println(ExcludePrivateAnnotationsJDiffDoclet.class.getSimpleName());
        return JDiff.start(RootDocProcessor.process(doc));
    }
    public static int optionLength(String option){
        Integer length = StabilityOptions.optionLength(option);
        if (length != null) {
            return length;
        }
        return JDiff.optionLength(option);
    }
    public static boolean validOptions(String[][] options, DocErrorReporter reporter){
        StabilityOptions.validOptions(options,reporter);
        String[][] filterOptions = StabilityOptions.filterOptions(options);
        return JDiff.validOptions(filterOptions,reporter);
    }
}
