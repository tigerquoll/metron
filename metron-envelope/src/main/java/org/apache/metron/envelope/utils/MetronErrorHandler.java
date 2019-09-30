package org.apache.metron.envelope.utils;

import org.apache.metron.common.error.MetronError;

public interface MetronErrorHandler {
  void handleError(MetronError error);
}
