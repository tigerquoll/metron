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
 * @param <D> Type of result
 */
public abstract class Either<E, D> {
  @NotNull
  public static <E, D> Either<E, D> NullableError(@Nullable E value) {
    return new NullableError<>(value);
  }

  @NotNull
  public static <E, D> Either<E, D> NullableResult(@Nullable D value) {
    return new NullableResult<>(value);
  }

  @NotNull
  public static <E, D> Either<E, D> Error(@NotNull E value) {
    return new Error<>(value);
  }

  @NotNull
  public static <E, D> Either<E, D> Data(@NotNull D value) {
    return new Result<>(value);
  }

  private Either() {}

  public abstract boolean isError();

  public abstract boolean isData();

  @Nullable
  public abstract E error();

  @Nullable
  public abstract D data();

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
  public abstract D getOrElse(@Nullable D elseValue);

  @NotNull
  public abstract Optional<D> getOption();

  @NotNull
  public abstract Stream<D> getDataStream();

  @NotNull
  public abstract <T> Optional <T> map(@NotNull Function<? super D,T> mapper) ;

  public abstract void forEach(@NotNull Consumer<D> action);

  /**
   * A Result value that may be null
   * @param <E> Error Type
   * @param <D> Result TYpe
   */
  private static class NullableResult<E, D> extends Either<E, D> {
    final D resultVal;

    NullableResult(@Nullable D result) {
      this.resultVal = result;
    }

    @Override
    public boolean isError() {
      return false;
    }

    @Override
    public boolean isData() {
      return true;
    }

    @Override
    @NotNull
    public E error() {
      throw new IllegalStateException("Attempting to extract error from a result");
    }

    @Override
    @Nullable
    public D data() {
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
    public D getOrElse(@Nullable D elseValue) {
      return resultVal;
    }

    @Override
    @NotNull
    public Optional<D> getOption() {
      return Optional.ofNullable(resultVal);
    }

    @Override
    @NotNull
    public Stream<D> getDataStream() {
      return Stream.of(resultVal);
    }

    @Override
    @NotNull
    public <T> Optional<T> map(@NotNull Function<? super D, T> mapper) {
      return Optional.ofNullable(mapper.apply(resultVal));
    }

    @Override
    public void forEach(@NotNull Consumer<D> action) {
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
   * @param <D> Result Type
   */
  private static class Result<E, D> extends NullableResult<E, D> {
    Result(@NotNull D result) {
      super(result);
    }

    @Override
    @NotNull
    public D data() {
      return Objects.requireNonNull(resultVal);
    }

    @Override
    @NotNull
    public D getOrElse(@Nullable D elseValue) {
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
   * @param <D> Result Type
   */
  private static class NullableError<E, D> extends Either<E, D> {
    final E errorVal;

    NullableError(@Nullable E error) {
      this.errorVal = error;
    }

    @Override
    public boolean isError() {return true;}

    @Override
    public boolean isData() {return false;}

    @Override
    @Nullable
    public E error() {
      return errorVal;
    }

    @Override
    @NotNull
    public D data() {
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
    public D getOrElse(@Nullable D elseValue){
      return elseValue;
    }

    @Override
    @NotNull
    public Optional<D> getOption() {
      return Optional.empty();
    }

    @Override
    @NotNull
    public Stream<D> getDataStream() {
      return Stream.empty();
    }

    @Override
    @NotNull
    public <T> Optional<T> map(@NotNull Function<? super D,T> mapper) {
      return Optional.empty();
    }

    @Override
    public void forEach(@NotNull Consumer<D> action) {
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
   * @param <D> Result Type
   */
  private static class Error<E, D> extends NullableError<E, D> {
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
