package org.apache.hadoop.metrics2.filter;

import com.apache.hadoop.classification.InterfaceAudience;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

import java.util.Map;


@InterfaceAudience.Private
public abstract class AbstractPatternFilter extends MetricsFilter {
    protected static final String INCLUDE_KEY="include";
    protected static final String EXCLUDE_KEY="exclude";
    protected static final String INCLUDE_TAGS_KEY="include.tags";
    protected static final String EXCLUDE_TAGS_KEY="exclude.tags";
    private Pattern includePattern;
    private Pattern excludePattern;
    private final Map<String ,Pattern> includeTagPatterns;
    private final Map<String ,Pattern> excludeTagPatterns;
    private final Pattern tagPattern=Pattern.compile("^(\\w+):(.*)");

    AbstractPatternFilter(){
        includeTagPatterns= Maps.newHashMap();
        excludeTagPatterns=Maps.newHashMap();
    }

    @Override
    public void init(SubsetConfiguration conf){
        String patternString = conf.getString(INCLUDE_KEY);
        if (patternString != null && !patternString.isEmpty()) {
            setIncludePattern(compile(patternString));
        }
        patternString=conf.getString(EXCLUDE_KEY);
        if (patternString != null && !patternString.isEmpty()) {
            setExcludePattern(compile(patternString));
        }
        String[] patternStrings = conf.getStringArray(INCLUDE_TAGS_KEY);
        if (patternStrings != null && patternStrings.length > 0) {
            for (String pstr : patternStrings) {
                Matcher matcher = tagPattern.matcher(pstr);
                if (!matcher.matches()) {
                    throw new MetricsException("Illegal tag pattern: "+pstr);
                }
                setIncludeTagPattern(matcher.group(1),compile(matcher.group(2)));
            }
        }
        patternStrings = conf.getStringArray(EXCLUDE_TAGS_KEY);
        if (patternStrings != null && patternStrings.length > 0) {
            for (String pstr : patternStrings) {
                Matcher matcher = tagPattern.matcher(pstr);
                if (!matcher.matches()) {
                    throw new MetricsException("Illegal tag pattern: "+pstr);
                }
                setExcludeTagPattern(matcher.group(1),compile(matcher.group(2)));
            }
        }
    }
    void setIncludePattern(Pattern includePattern){
        this.includePattern=includePattern;
    }

    public void setExcludePattern(Pattern excludePattern) {
        this.excludePattern = excludePattern;
    }
    void setIncludeTagPattern(String name,Pattern pattern){
        includeTagPatterns.put(name,pattern);
    }
    void setExcludeTagPattern(String name,Pattern pattern){
        excludeTagPatterns.put(name,pattern);
    }
    @Override
    public boolean accepts(MetricsTag tag){
        Pattern ipat = includeTagPatterns.get(tag.name());
        if (ipat != null && ipat.matcher(tag.value()).matches()) {
            return true;
        }
        Pattern epat=excludeTagPatterns.get(tag.name());
        if (epat != null && epat.matcher(tag.value()).matches()) {
            return false;
        }
        if (!includeTagPatterns.isEmpty() && excludeTagPatterns.isEmpty()) {
            return false;
        }
        return true;
    }
    @Override
    public boolean accepts(Iterable<MetricsTag> tags){
        for (MetricsTag tag : tags) {
            Pattern ipat = includeTagPatterns.get(tag.name());
            if (ipat != null && ipat.matcher(tag.value()).matches()) {
                return true;
            }
            Pattern epat=excludeTagPatterns.get(tag.name());
            if (epat != null && epat.matcher(tag.value()).matches()) {
                return false;
            }
        }
        if (!includeTagPatterns.isEmpty() && excludeTagPatterns.isEmpty()) {
            return false;
        }
        return true;
    }

    @Override
    public boolean accepts(String name){
        if (includePattern != null && includePattern.matcher(name).matches()) {
            return true;
        }
        if (excludePattern != null && excludePattern.matcher(name).matches()) {
            return false;
        }
        if (includePattern!=null && excludePattern!=null) {
            return false;
        }
        return true;
    }
    protected abstract Pattern compile(String s);

}
