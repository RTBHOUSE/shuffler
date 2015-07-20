package com.rtbhouse.shuffler;

import java.io.Serializable;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.crunch.FilterFn;
import org.apache.crunch.MapFn;

/**
 * @author br
 */
public class CrunchUtils {

    public static <S, T> MapFn<S, T> mapFn(SerializableFunction<S, T> lambda) {
        return new MapFn<S, T>() {
            @Override
            public T map(S input) {
                return lambda.apply(input);
            }
        };
    }

    public static <T> FilterFn<T> filterFn(SerializablePredicate<T> lambda) {
        return new FilterFn<T>() {
            @Override
            public boolean accept(T input) {
                return lambda.test(input);
            }
        };
    }

    public interface SerializableFunction<S, T> extends Function<S, T>, Serializable {
    }

    public interface SerializablePredicate<T> extends Predicate<T>, Serializable {
    }
}
