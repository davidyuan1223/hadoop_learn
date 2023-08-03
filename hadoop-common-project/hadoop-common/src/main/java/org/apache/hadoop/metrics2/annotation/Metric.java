package org.apache.hadoop.metrics2.annotation;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import java.lang.annotation.*;

@InterfaceAudience.Public
@InterfaceStability.Evolving
@Documented
@Target({ElementType.TYPE,ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Metric {
    public enum Type{
        DEFAULT,COUNTER,GAUGE,TAG
    }
    String[] value() default {};
    String about() default "";
    String sampleName() default "Ops";
    String valueName() default "Time";
    boolean always() default false;
    Type type() default Type.DEFAULT;
    int interval() default 10;
}
