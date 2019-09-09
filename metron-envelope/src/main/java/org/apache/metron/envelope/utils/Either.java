package org.apache.metron.envelope.utils;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A value that contains either a result value or an error value
 * These are immutable classes, sub-classes are used for Error and Success
 * @param <E> Type of error
 * @param <R> Type of result
 */
public abstract class Either<E, R> {
  @NotNull
  public static <E, R> Either<E, R> NullableError(@Nullable E value) {
    return new NullableError<>(value);
  }

  @NotNull
  public static <E, R> Either<E, R> NullableResult(@Nullable R value) {
    return new NullableResult<>(value);
  }

  @NotNull
  public static <E, R> Either<E, R> Error(@NotNull E value) {
    return new Error<>(value);
  }

  @NotNull
  public static <E, R> Either<E, R> Result(@NotNull R value) {
    return new Result<>(value);
  }

  private Either() {}

  public abstract boolean isError();

  public abstract boolean isResult();

  @Nullable
  public abstract E error();

  @Nullable
  public abstract R result();

  @Nullable
  public abstract E errorGetOrElse(@Nullable E elseValue);

  @NotNull
  public abstract Optional<E> errorToOption();

  @NotNull
  public abstract Stream<E> getErrorStream();

  @NotNull
  public abstract <T> Optional <T> errorMap(@NotNull Function <? super E,T> mapper);

  public abstract void errorForEach(@NotNull Consumer <E> action);

  @Nullable
  public abstract R getOrElse(@Nullable R elseValue);

  @NotNull
  public abstract Optional<R> getOption();

  @NotNull
  public abstract Stream<R> getStream();

  @NotNull
  public abstract <T> Optional <T> map(@NotNull Function<? super R,T> mapper) ;

  public abstract void forEach(@NotNull Consumer<R> action);

  /**
   * A Result value that may be null
   * @param <E> Error Type
   * @param <R> Result TYpe
   */
  private static class NullableResult<E,R> extends Either<E,R> {
    final R resultVal;

    NullableResult(@Nullable R result) {
      this.resultVal = result;
    }

    @Override
    public boolean isError() {
      return false;
    }

    @Override
    public boolean isResult() {
      return true;
    }

    @Override
    @NotNull
    public E error() {
      throw new IllegalStateException("Attempting to extract error from a result");
    }

    @Override
    @Nullable
    public R result() {
      return resultVal;
    }

    @Override
    @Nullable
    public E errorGetOrElse(@Nullable E elseValue) {
      return elseValue;
    }

    @Override
    @NotNull
    public Optional<E> errorToOption() {
      return Optional.empty();
    }

    @Override
    @NotNull
    public Stream<E> getErrorStream() {
      return Stream.empty();
    }

    @Override
    @NotNull
    public <T> Optional<T> errorMap(@NotNull Function<? super E, T> mapper) {
      return Optional.empty();
    }

    @Override
    public void errorForEach(@NotNull Consumer<E> action) {
    }

    @Override
    @Nullable
    public R getOrElse(@Nullable R elseValue) {
      return resultVal;
    }

    @Override
    @NotNull
    public Optional<R> getOption() {
      return Optional.ofNullable(resultVal);
    }

    @Override
    @NotNull
    public Stream<R> getStream() {
      return Stream.of(resultVal);
    }

    @Override
    @NotNull
    public <T> Optional<T> map(@NotNull Function<? super R, T> mapper) {
      return Optional.ofNullable(mapper.apply(resultVal));
    }

    @Override
    public void forEach(@NotNull Consumer<R> action) {
      action.accept(resultVal);
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NullableResult<?, ?> that = (NullableResult<?, ?>) o;
      return Objects.equals(resultVal, that.resultVal);
    }

    @Override
    public int hashCode() {
      return Objects.hash(resultVal);
    }

    @Override
    public String toString() {
      return "NullableResult{" +
              "resultVal=" + resultVal +
              '}';
    }
  }

  /**
   * A result value that cannot be null
   * @param <E> Error Type
   * @param <R> Result Type
   */
  private static class Result<E,R> extends NullableResult<E,R> {
    Result(@NotNull R result) {
      super(result);
    }

    @Override
    @NotNull
    public R result() {
      return Objects.requireNonNull(resultVal);
    }

    @Override
    @NotNull
    public R getOrElse(@Nullable R elseValue) {
      return Objects.requireNonNull(resultVal);
    }

    @Override
    public String toString() {
      return "Result{" +
              "resultVal=" + resultVal +
              '}';
    }
  }

  /**
   * An Error Value that can be null
   * @param <E> Error Type
   * @param <R> Result Type
   */
  private static class NullableError<E,R> extends Either<E,R> {
    final E errorVal;

    NullableError(@Nullable E error) {
      this.errorVal = error;
    }

    @Override
    public boolean isError() {return true;}

    @Override
    public boolean isResult() {return false;}

    @Override
    @Nullable
    public E error() {
      return errorVal;
    }

    @Override
    @NotNull
    public R result() {
      throw new IllegalStateException("Attempting to extract result from an error");
    }

    @Override
    @Nullable
    public E errorGetOrElse(@Nullable E elseValue){
      return errorVal;
    }

    @Override
    @NotNull
    public Optional<E> errorToOption() {
      return Optional.ofNullable(errorVal);
    }

    @Override
    @NotNull
    public Stream<E> getErrorStream() {
      return Stream.of(errorVal);
    }

    @Override
    @NotNull
    public <T> Optional<T> errorMap(@NotNull Function<? super E,T> mapper) {
      return Optional.ofNullable(mapper.apply(errorVal));
    }

    @Override
    public void errorForEach(@NotNull Consumer <E> action) {
       action.accept(errorVal);
    }

    @Override
    @Nullable
    public R getOrElse(@Nullable R elseValue){
      return elseValue;
    }

    @Override
    @NotNull
    public Optional<R> getOption() {
      return Optional.empty();
    }

    @Override
    @NotNull
    public Stream<R> getStream() {
      return Stream.empty();
    }

    @Override
    @NotNull
    public <T> Optional<T> map(@NotNull Function<? super R,T> mapper) {
      return Optional.empty();
    }

    @Override
    public void forEach(@NotNull Consumer<R> action) {
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NullableError<?, ?> that = (NullableError<?, ?>) o;
      return Objects.equals(errorVal, that.errorVal);
    }

    @Override
    public int hashCode() {
      return Objects.hash(errorVal);
    }

    @Override
    public String toString() {
      return "NullableError{" +
              "errorVal=" + errorVal +
              '}';
    }
  }

  /**
   * An error Value that cannot be null
   * @param <E> Error TYpe
   * @param <R> Result Type
   */
  private static class Error<E,R> extends NullableError<E,R> {
    Error(@NotNull E error) {
      super(error);
    }

    @Override
    @NotNull
    public E error() {
      return Objects.requireNonNull(errorVal);
    }

    @Override
    @NotNull
    public E errorGetOrElse(@Nullable E elseValue){
      return Objects.requireNonNull(errorVal);
    }

    @Override
    public String toString() {
      return "Error{" +
              "errorVal=" + errorVal +
              '}';
    }
  }

}
