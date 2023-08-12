package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;

import javax.annotation.Nonnull;
import java.io.IOException;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface FSBuilder<S, B extends FSBuilder<S,B>> {
    B opt(@Nonnull String key, @Nonnull String value);

    B opt(@Nonnull String key,@Nonnull boolean value);

    B opt(@Nonnull String key,@Nonnull int value);

    B opt(@Nonnull String key,@Nonnull float value);

    B opt(@Nonnull String key,@Nonnull long value);

    B opt(@Nonnull String key,@Nonnull double value);

    B opt(@Nonnull String key,@Nonnull String... value);

    B must(@Nonnull String key,@Nonnull String  value);

    B must(@Nonnull String key,@Nonnull int  value);

    B must(@Nonnull String key,@Nonnull float  value);

    B must(@Nonnull String key,@Nonnull long  value);

    B must(@Nonnull String key,@Nonnull double  value);

    B must(@Nonnull String key,@Nonnull String...  value);

    B must(@Nonnull String key,@Nonnull boolean  value);

    S build() throws IllegalArgumentException,UnsupportedOperationException, IOException;
}
