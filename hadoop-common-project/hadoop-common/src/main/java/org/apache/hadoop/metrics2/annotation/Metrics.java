package org.apache.hadoop.metrics2.annotation;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.lang.annotation.*;

@InterfaceAudience.Public
@InterfaceStability.Evolving
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Metrics {
    String name() default "";
    String about() default "";
    String context();
}
