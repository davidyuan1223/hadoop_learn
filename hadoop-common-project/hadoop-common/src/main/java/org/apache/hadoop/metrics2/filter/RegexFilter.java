package org.apache.hadoop.metrics2.filter;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.google.re2j.Pattern;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RegexFilter extends AbstractPatternFilter{
    @Override
    protected Pattern compile(String s) {
        return Pattern.compile(s);
    }
}
