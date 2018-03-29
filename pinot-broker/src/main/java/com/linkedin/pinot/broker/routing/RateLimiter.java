package com.linkedin.pinot.broker.routing;

public interface RateLimiter {

  public boolean take();
}
