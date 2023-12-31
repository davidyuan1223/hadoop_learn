/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.apache.hadoop.classification.tools;

import com.sun.javadoc.DocErrorReporter;
import com.sun.javadoc.LanguageVersion;
import com.sun.javadoc.RootDoc;
import com.sun.tools.doclets.standard.Standard;

/**
 * A <a href="http://java.sun.com/javase/6/docs/jdk/api/javadoc/doclet/">Doclet</a>
 * for excluding elements that are annotated with
 * It delegates to the Standard Doclet, and takes the same options.
 */
public class ExcludePrivateAnnotationsStandardDoclet {

  public static LanguageVersion languageVersion() {
    return LanguageVersion.JAVA_1_5;
  }

  public static boolean start(RootDoc root) {
    System.out.println(
        ExcludePrivateAnnotationsStandardDoclet.class.getSimpleName());
    RootDoc excludedDoc = RootDocProcessor.process(root);
    if (excludedDoc.specifiedPackages().length == 0) {
      return true;
    }
    return Standard.start(excludedDoc);
  }

  public static int optionLength(String option) {
    Integer length = StabilityOptions.optionLength(option);
    if (length != null) {
      return length;
    }
    return Standard.optionLength(option);
  }

  public static boolean validOptions(String[][] options,
      DocErrorReporter reporter) {
    StabilityOptions.validOptions(options, reporter);
    String[][] filteredOptions = StabilityOptions.filterOptions(options);
    return Standard.validOptions(filteredOptions, reporter);
  }
}
