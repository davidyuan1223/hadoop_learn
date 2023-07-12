package com.apache.hadoop.classification;

import java.lang.annotation.*;

/**
 * Annotates a program element that exists,or is more widely visible than otherwise necessary,specifically for
 * use in test code. more precisely test code within the hadoop-*modules. moreover,this gives the implicit
 * scope and stability of:
 *      InterfaceAudience.Private
 *      InterfaceStability.Unstable
 * if external modules need to access/override these modules,then they must be re-scope as public/limitedprivate
 */
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.TYPE,ElementType.METHOD,ElementType.FIELD,ElementType.CONSTRUCTOR})
@Documented
public @interface VisibleForTesting {
}
