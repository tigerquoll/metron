package org.apache.metron.envelope.utils;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

public final class ClassUtils {

  private ClassUtils() {}

  @NotNull
  public static Object instantiateClass(@NotNull String serialisationFactoryName) {
    Objects.requireNonNull(serialisationFactoryName);
    try {
      @NotNull final Class<?> myClass = Objects.requireNonNull(Class.forName(serialisationFactoryName));
      @NotNull final Constructor constructor = Objects.requireNonNull(myClass.getConstructor());
      return Objects.requireNonNull(constructor.newInstance());
    } catch (NoSuchMethodException | ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(String.format("Error attempt to create class of type %s", serialisationFactoryName),e);
    }
  }
}
