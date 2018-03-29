package com.linkedin.pinot.broker.queryquota;

import com.google.common.util.concurrent.RateLimiter;
import com.linkedin.pinot.common.config.QuotaConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


public class TableQueryQuotaManager {

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final Map<String, RateLimiter> _rateLimiterMap;
  private static double DEFAULT_QPS = 1.0;

  public TableQueryQuotaManager(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
    _rateLimiterMap  = new ConcurrentHashMap<>();
  }

  public boolean acquire(String tableName) {
    RateLimiter rateLimiter = _rateLimiterMap.get(tableName);
    if (rateLimiter == null) {
      double rate = DEFAULT_QPS;

      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, offlineTableName);
      if (tableConfig == null) {
        tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, TableNameBuilder.REALTIME.tableNameWithType(tableName));
      }
      assert tableConfig != null;
      QuotaConfig quotaConfig = tableConfig.getQuotaConfig();
      if (quotaConfig != null) {
        // TODO: use qps from quota
        // rate = quotaConfig.getQps();
      }

      rateLimiter = RateLimiter.create(rate);
      _rateLimiterMap.put(tableName, rateLimiter);
    }
    return rateLimiter.tryAcquire();
  }

  public void processQueryQuotaChange() {
    // TODO: update rate
    ;
  }
}
