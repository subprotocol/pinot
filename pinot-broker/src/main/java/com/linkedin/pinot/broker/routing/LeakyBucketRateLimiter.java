package com.linkedin.pinot.broker.routing;

import java.util.concurrent.TimeUnit;


public class LeakyBucketRateLimiter implements RateLimiter {

  private int _capacity;
  private int _tokenPerSecond;
  private int _numTokens;
  private int _numBrokers;
  private long _timestamp;

  public LeakyBucketRateLimiter(int tokenPerUnit, TimeUnit unit) {
    _capacity = _tokenPerSecond = (int) (tokenPerUnit / unit.toSeconds(1L));
    _timestamp = System.currentTimeMillis();
  }

  @Override
  public boolean take() {
    long now = System.currentTimeMillis();
    _numTokens += (int) ((now - _timestamp) * _tokenPerSecond / 1000);
    if (_numTokens > _capacity) {
      _numTokens = _capacity;
    }
    _timestamp = now;
    if (_numTokens < 1) {
      return false;
    }
    _numTokens--;
    return true;
  }
}
