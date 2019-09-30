package org.apache.metron.envelope.utils;

import org.apache.metron.common.Constants;
import org.apache.metron.common.error.MetronError;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.function.Function;

public class MetronErrorUtils {
  public static <T,R> Function<T, Either<MetronError, R>> convertErrorsToMetronErrors(
          @NotNull Constants.ErrorType MetronErrorType,
          @NotNull ErrorUtils.ThrowingFunction<T,R> function) {
    return t -> {
      try {
        return Either.Result(Objects.requireNonNull(function.apply(t)));
      } catch (Exception ex) {
        return Either.Error(new MetronError()
                .withErrorType(MetronErrorType)
                .withThrowable(ex)
                .withMessage("Field values: " + x.getCauseStringOr("Null Messages")));
      }
    };
  }
}
