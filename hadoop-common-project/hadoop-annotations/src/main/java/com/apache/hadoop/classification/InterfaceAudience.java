package com.apache.hadoop.classification;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to inform uses of a package,class or method's intended audience.Currently the audience can
 * be Public,LimitedPrivate,Private.All public classes must have InterfaceAudience annotation
 * 1. Public classes that are not marked with this annotation must be considered by default as Private
 * 2. External applications must only use classes that are marked Public.Avoid using non public classes
 * as these classes could be removed or change in incompatible ways.
 * 3. Hadoop projects must only use classes that are marked LimitedPrivate or Public
 * 4. Methods may have different annotation that it is more restrictive compared to the audience classification
 * of the class.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class InterfaceAudience {
    /**
     * Intended for use by any project or application
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Public {};
    /**
     * Intended for the project specified in the annotation
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    public @interface LimitedPrivate {
        String[] value();
    };
    /**
     * Intended for use only within Hadoop itself
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Private {};

    private InterfaceAudience() {}
}
