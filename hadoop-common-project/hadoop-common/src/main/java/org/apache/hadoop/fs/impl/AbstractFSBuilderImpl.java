package org.apache.hadoop.fs.impl;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandler;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.*;

@InterfaceAudience.Public
@InterfaceStability.Unstable
/**
 * @Description: TODO
 * @Author: yuan
 * @Date: 2023/07/30
 **/
public abstract class AbstractFSBuilderImpl<S,B extends FSBuilder<S,B>> implements FSBuilder<S,B> {
    public static final String UNKNOWN_MANDATORY_KEY=
            "Unknown mandatory key";
    @VisibleForTesting
    public static final String E_BOTH_A_PATH_AND_A_PATH_HANDLE=
            "Both a path and a pathHandle has been provided to the constructor";
    private final Optional<Path> optionalPath;
    private final Optional<PathHandler> optionalPathHandler;
    private final Configuration options=new Configuration(false);
    private final Set<String > mandatoryKeys=new HashSet<>();
    private final Set<String > optionalKeys=new HashSet<>();
    protected AbstractFSBuilderImpl(@Nonnull Optional<Path> optionalPath,
                                    @Nonnull Optional<PathHandler> optionalPathHandler){
        Preconditions.checkArgument(!(Preconditions.checkNotNull(optionalPath).isPresent()
        &&Preconditions.checkNotNull(optionalPathHandler).isPresent()));
        this.optionalPath=optionalPath;
        this.optionalPathHandler=optionalPathHandler;
    }

    protected AbstractFSBuilderImpl(@Nonnull final Path path){
        this(Optional.of(path),Optional.empty());
    }

    protected AbstractFSBuilderImpl(@Nonnull final PathHandler pathHandler){
        this(Optional.empty(),Optional.of(pathHandler));
    }

    public B getThisBuilder(){
        return (B)this;
    }

    public Optional<Path> getOptionalPath() {
        return optionalPath;
    }

    public Path getPath(){
        return optionalPath.get();
    }

    public Optional<PathHandler> getOptionalPathHandler() {
        return optionalPathHandler;
    }

    public PathHandler getPathHandler(){
        return optionalPathHandler.get();
    }

    @Override
    public B opt(@Nonnull String key, @Nonnull String value) {
        mandatoryKeys.remove(key);
        optionalKeys.add(key);
        options.set(key,value);
        return getThisBuilder();
    }

    @Override
    public B opt(@Nonnull final String key, boolean value) {
        mandatoryKeys.remove(key);
        optionalKeys.add(key);
        options.setBoolean(key, value);
        return getThisBuilder();
    }

    /**
     * Set optional int parameter for the Builder.
     *
     * @see #opt(String, String)
     */
    @Override
    public B opt(@Nonnull final String key, int value) {
        mandatoryKeys.remove(key);
        optionalKeys.add(key);
        options.setInt(key, value);
        return getThisBuilder();
    }

    @Override
    public B opt(@Nonnull final String key, final long value) {
        mandatoryKeys.remove(key);
        optionalKeys.add(key);
        options.setLong(key, value);
        return getThisBuilder();
    }

    /**
     * Set optional float parameter for the Builder.
     *
     * @see #opt(String, String)
     */
    @Override
    public B opt(@Nonnull final String key, float value) {
        mandatoryKeys.remove(key);
        optionalKeys.add(key);
        options.setFloat(key, value);
        return getThisBuilder();
    }

    /**
     * Set optional double parameter for the Builder.
     *
     * @see #opt(String, String)
     */
    @Override
    public B opt(@Nonnull final String key, double value) {
        mandatoryKeys.remove(key);
        optionalKeys.add(key);
        options.setDouble(key, value);
        return getThisBuilder();
    }

    /**
     * Set an array of string values as optional parameter for the Builder.
     *
     * @see #opt(String, String)
     */
    @Override
    public B opt(@Nonnull final String key, @Nonnull final String... values) {
        mandatoryKeys.remove(key);
        optionalKeys.add(key);
        options.setStrings(key, values);
        return getThisBuilder();
    }

    @Override
    public B must(@Nonnull String key, @Nonnull String value) {
        mandatoryKeys.add(key);
        options.set(key,value);
        return getThisBuilder();
    }

    @Override
    public B must(@Nonnull final String key, boolean value) {
        mandatoryKeys.add(key);
        optionalKeys.remove(key);
        options.setBoolean(key, value);
        return getThisBuilder();
    }

    /**
     * Set mandatory int option.
     *
     * @see #must(String, String)
     */
    @Override
    public B must(@Nonnull final String key, int value) {
        mandatoryKeys.add(key);
        optionalKeys.remove(key);
        options.setInt(key, value);
        return getThisBuilder();
    }

    @Override
    public B must(@Nonnull final String key, final long value) {
        mandatoryKeys.add(key);
        optionalKeys.remove(key);
        options.setLong(key, value);
        return getThisBuilder();
    }

    /**
     * Set mandatory float option.
     *
     * @see #must(String, String)
     */
    @Override
    public B must(@Nonnull final String key, float value) {
        mandatoryKeys.add(key);
        optionalKeys.remove(key);
        options.setFloat(key, value);
        return getThisBuilder();
    }

    /**
     * Set mandatory double option.
     *
     * @see #must(String, String)
     */
    @Override
    public B must(@Nonnull final String key, double value) {
        mandatoryKeys.add(key);
        optionalKeys.remove(key);
        options.setDouble(key, value);
        return getThisBuilder();
    }

    /**
     * Set a string array as mandatory option.
     *
     * @see #must(String, String)
     */
    @Override
    public B must(@Nonnull final String key, @Nonnull final String... values) {
        mandatoryKeys.add(key);
        optionalKeys.remove(key);
        options.setStrings(key, values);
        return getThisBuilder();
    }

    public Configuration getOptions() {
        return options;
    }

    public Set<String> getMandatoryKeys() {
        return Collections.unmodifiableSet(mandatoryKeys);
    }

    public Set<String> getOptionalKeys() {
        return Collections.unmodifiableSet(optionalKeys);
    }

    protected void rejectUnknownMandatoryKeys(final Collection<String> knownKeys,
                                              String extraErrorText)throws IllegalArgumentException{
        rejectUnknownMandatoryKeys(mandatoryKeys,knownKeys,extraErrorText);
    }

    public static void rejectUnknownMandatoryKeys(final Set<String > mandatoryKeys,
                                                  final Collection<String > knownKeys,
                                                  final String extraErrorText)
        throws IllegalArgumentException{
        final String eText=extraErrorText.isEmpty()?"":(extraErrorText+"");
        mandatoryKeys.forEach((key)->{
            Preconditions.checkArgument(knownKeys.contains(key),
                    UNKNOWN_MANDATORY_KEY+" %s\"%s\"",eText,key);
        });
    }
}
