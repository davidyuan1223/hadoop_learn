package com.apache.hadoop.classification;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to inform users of how must to rely on a particular,class or method not changing over time.
 * Currently the stability can be Stable,Evolving,Unstable
 * 1. All classes that are annotated with InterfaceAudience's Public or LimitedPrivate must have interfaceStability annotation
 * 2. Classes that are InterfaceAudience.Private are to be considered unstable unless a different InterfaceStability annotation states otherwise
 * 3. Incompatible changes must not be made to classes marked as stable
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class InterfaceStability {
    /**
     * can evolve while retaining compatibility for minor release boundaries;
     * can break compatibility only at major release
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Stable {};
    /**
     * Evolving,but can break compatibility at minor release
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Evolving {};
    /**
     * no guarantee is provided as to reliability or stability across ant level of release granularity
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Unstable {};
}
