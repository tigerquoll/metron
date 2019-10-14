package org.apache.metron.envelope.utils;

import org.apache.metron.common.Constants;
import org.apache.metron.common.error.MetronError;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.simple.JSONObject;

import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;

/**
 * Utility classes to make error handling inside lambdas a lot cleaner
 */
public class ErrorUtils {
  public static void checkState(boolean validState) {
    checkState(validState, "illegal state detected");
  }

  public static void checkState(boolean validState, String message) throws IllegalStateException {
    if (!validState) {
      throw new IllegalStateException(message);
    }
  }

  /**
   * Specialised Pair class to contain Error Information
   * @param <E> Type of the Exception
   * @param <C> Type of the source Data that caused the Exception
   */
  public static class ErrorInfo<E,C> {
    @Nullable private final E exception;
    @Nullable private final C cause;

    private ErrorInfo(@Nullable E exception, @Nullable C cause) {
      this.exception = exception;
      this.cause = cause;
    }

    @NotNull
    public static <E,S> ErrorInfo<E,S> of(@Nullable E exception, @Nullable S cause) {
      return new ErrorInfo<>(exception,cause);
    }

    @Nullable
    public E getException() {
      return exception;
    }

    @NotNull
    public String getExceptionStringOr(String message) {
      return exception == null ? message : exception.toString();
    }

    @Nullable
    public C getCause() {
      return cause;
    }

    @NotNull
    public String getCauseStringOr(String message) {
      return cause == null ? message : cause.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ErrorInfo<?, ?> errorInfo = (ErrorInfo<?, ?>) o;
      return Objects.equals(getException(), errorInfo.getException()) &&
              Objects.equals(getCause(), errorInfo.getCause());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getException(), getCause());
    }

    @Override
    public String toString() {
      return "ErrorInfo{" +
              "exception=" + exception +
              ", cause=" + cause +
              '}';
    }
  }

  /**
   * A throwing function is a function that throws an Exception
   * @param <T> Input type of function
   * @param <R> Return type of function
   */
  @FunctionalInterface
  public interface ThrowingFunction<T,R> {
    @Nullable R apply(@Nullable T t) throws Exception;
  }

  /**
   * Transforms a ThrowingFunction into a normal Function that returns an Either<Exception,Result>
   *   The transformed function will no longer throw exceptions
   * @param function Throwing function to wrap
   * @param <T> Input Type of function
   * @param <R> Result Type of function
   * @return A function that returns Either<Exception, R>
   */
  @NotNull
  public static <T,R> Function<T, Either<Exception,R>> catchErrors(@NotNull ThrowingFunction<T,R> function) {
    return t -> {
      try {
        return Either.NullableResult(function.apply(t));
      } catch (Exception ex) {
        return Either.Error(ex);
      }
    };
  }

  /**
   * Transforms a ThrowingFunction into a normal Function that returns an Either<Exception,Result>
   *   Returned nulls will be treated as a Null Pointer Exception and wrapped in an Error
   *   The transformed function will no longer throw exceptions
   * @param function Throwing function to wrap
   * @param <T> Input Type of function
   * @param <R> Result Type of function
   * @return A function that returns Either<Exception, R>
   */
  @NotNull
  public static <T,R> Function<T, Either<Exception,R>> catchErrorsAndNulls(@NotNull ThrowingFunction<T,R> function) {
    return t -> {
      try {
        return Either.Data(Objects.requireNonNull(function.apply(t)));
      } catch (Exception ex) {
        return Either.Error(ex);
      }
    };
  }

  /**
   * Transforms a ThrowingFunction into a normal Function that returns an Either<ErrorInfo<Exception,SourceData>,Result>
   *   This allows the a full copy of the data that caused the exception to be bundled up with the exception itself,
   *   to allow for easier debugging
   *   The transformed function will no longer throw exceptions
   * @param function Throwing function to wrap
   * @param <T> Input Type of function
   * @param <R> Result Type of function
   * @return A function that returns Either<ErrorInfo<Exception,T>,R>
   */
  @NotNull
  public static <T,R> Function<T, Either<ErrorInfo<Exception,T>,R>> catchErrorsWithCause(@NotNull ThrowingFunction<T,R> function) {
    return t -> {
      try {
        return Either.NullableResult(function.apply(t));
      } catch (Exception ex) {
        return Either.Error(ErrorInfo.of(ex,t));
      }
    };
  }

  /**
   * Transforms a ThrowingFunction into a normal Function that returns an Either<ErrorInfo<Exception,SourceData>,Result>
   *   This allows the a full copy of the data that caused the exception to be bundled up with the exception itself,
   *   to allow for easier debugging. The transformed function will no longer throw exceptions.
   *   Null values will cause a NullPointerException which will be wrapped in an Error
   * @param function Throwing function to wrap
   * @param <T> Input Type of function
   * @param <R> Result Type of function
\  * @return A function that returns Either<ErrorInfo<Exception,T>,R>
   */
  @NotNull
  public static <T,R> Function<T, Either<ErrorInfo<Exception,T>,R>> catchErrorsAndNullsWithCause(@NotNull ThrowingFunction<T,R> function) {
    return t -> {
      try {
        return Either.Data(Objects.requireNonNull(function.apply(t)));
      } catch (Exception ex) {
        return Either.Error(ErrorInfo.of(ex,t));
      }
    };
  }


  /**
   * Mapping Function specialised to deal with JSONObjects and MetronErrors
   * @param MetronErrorType If exceptions are thrown, what MetronErrorType should they be mapped to
   * @param function Function that processes JSONObject
   * @param <T> Mapping function takes this as input
   * @param <R> Mapping function returns this as output
   * @return  Either<MetronError, R>
   */
  public static <T extends JSONObject,R> Function<T, Either<MetronError, R>> convertErrorsToMetronErrors(
          @NotNull Constants.ErrorType MetronErrorType,
          @NotNull ErrorUtils.ThrowingFunction<T,R> function) {
    return t -> {
      try {
        return Either.Data(Objects.requireNonNull(function.apply(t)));
      } catch (Exception ex) {
        String sensor = t.getOrDefault(Constants.SENSOR_TYPE, Constants.ERROR_TYPE).toString();
        return Either.Error(new MetronError()
                .withErrorType(MetronErrorType)
                .withThrowable(ex)
                .withSensorType(Collections.singleton(sensor))
                .withMessage(String.format("Field values: %s", t.toJSONString())));
      }
    };
  }

  /**
   * Mapping Function specialised to deal with JSONObjects and MetronErrors
   * @param MetronErrorType If exceptions are thrown, what MetronErrorType should they be mapped to
   * @param function Function that processes JSONObject
   * @param <T> Mapping function takes this as input
   * @param <R> Mapping function returns this as output
   * @return  Either<MetronError, R>
   */
  public static <T extends Row,R> Function<T, Either<MetronError, R>> convertSparkErrorsToMetronErrors(
          @NotNull Constants.ErrorType MetronErrorType,
          @NotNull ErrorUtils.ThrowingFunction<T,R> function) {
    return t -> {
      try {
        return Either.Data(Objects.requireNonNull(function.apply(t)));
      } catch (Exception ex) {
        return Either.Error(new MetronError()
                .withErrorType(MetronErrorType)
                .withThrowable(ex)
                .withSensorType(Collections.singleton(Constants.ERROR_TYPE))
                .withMessage(String.format("Field values: %s", t.toString())));
      }
    };
  }
}
