(ns io.pithos.loader
  (:require [io.pithos.config      :as config]
            [clojure.tools.logging :refer [debug]])
  (:import org.apache.cassandra.config.Config
           org.apache.cassandra.config.SeedProviderDef
           org.apache.cassandra.config.Config$CommitLogSync
           org.apache.cassandra.config.Config$MemtableAllocationType
           org.apache.cassandra.config.Config$InternodeCompression
           java.util.LinkedHashMap)
  (:gen-class
   :name io.pithos.PithosConfigLoader
   :implements [org.apache.cassandra.config.ConfigurationLoader]
   :main false))

(def default-config
  {:cluster_name "embedded cluster"
   :hinted_handoff_enabled "true"
   :max_hint_window_in_ms 10800000
   :hinted_handoff_throttle_in_kb 1024
   :max_hints_delivery_threads 2
   :authenticator "AllowAllAuthenticator"
   :authorizer "AllowAllAuthorizer"
   :permissions_validity_in_ms 2000
   :partitioner "org.apache.cassandra.dht.Murmur3Partitioner"
   :data_file_directories [ "/var/db/pithos/data" ]
   :commitlog_directory   "/var/db/pithos/commitlog"
   :key_cache_size_in_mb 0
   :key_cache_save_period 0
   :row_cache_size_in_mb 0
   :row_cache_save_period 0
   :counter_cache_size_in_mb 0
   :counter_cache_save_period 0
   :saved_caches_directory "/var/db/pithos/saved_caches"
   :commitlog_sync "periodic"
   :commitlog_sync_period_in_ms 10000
   :commitlog_segment_size_in_mb 32
   :concurrent_reads 32
   :concurrent_writes 32
   :concurrent_counter_writes 32
   :memtable_allocation_type "heap_buffers"
   :index_summary_resize_interval_in_minutes 60
   :trickle_fsync false
   :trickle_fsync_interval_in_kb 10240
   :listen_address "127.0.0.1"
   :start_native_transport true
   :native_transport_port 9042
   :start_rpc false
   :rpc_address "localhost"
   :rpc_port 9160
   :rpc_keepalive true
   :rpc_server_type "sync"
   :thrift_framed_transport_size_in_mb 15
   :incremental_backups false
   :snapshot_before_compaction false
   :auto_snapshot true
   :tombstone_warn_threshold 1000
   :tombstone_failure_threshold 100000
   :column_index_size_in_kb 64
   :batch_size_warn_threshold_in_kb 5
   :compaction_throughput_mb_per_sec 16
   :sstable_preemptive_open_interval_in_mb 50
   :read_request_timeout_in_ms 5000
   :range_request_timeout_in_ms 10000
   :write_request_timeout_in_ms 2000
   :counter_write_request_timeout_in_ms 5000
   :cas_contention_timeout_in_ms 1000
   :truncate_request_timeout_in_ms 60000
   :request_timeout_in_ms 10000
   :cross_node_timeout false
   :endpoint_snitch "SimpleSnitch"
   :dynamic_snitch_update_interval_in_ms 100
   :dynamic_snitch_reset_interval_in_ms 600000
   :dynamic_snitch_badness_threshold 0.1
   :request_scheduler "org.apache.cassandra.scheduler.NoScheduler"
   :internode_compression "all"
   :inter_dc_tcp_nodelay false
   :seed_provider {"class_name" "org.apache.cassandra.locator.SimpleSeedProvider"
                   "parameters" [{"seeds" "127.0.0.1"}]}})

(defn set-field!
  [config key value]
  (debug "setting config field " key)
  (let [f  (.getDeclaredField (class config) (name key))
        re #"Can not set (int|java.lang.Integer) field (.*) to java.lang.Long"]
    (try
      (.set f config value)
      (catch IllegalArgumentException iae
        (if (re-find re (.getMessage iae))
          (.set f config (int value))
          (throw iae))))))

(defmulti apply-field (comp second vector))

(defmethod apply-field :data_file_directories
  [config key value]
  (set-field! config key (into-array String value)))

(defmethod apply-field :commitlog_sync
  [config key value]
  (let [sync-types {"periodic" Config$CommitLogSync/periodic
                    "batch" Config$CommitLogSync/batch}]
    (set-field! config key (get sync-types value))))

(defmethod apply-field :memtable_allocation_type
  [config key value]
  (let [ma-types {"offheap_buffers"
                  Config$MemtableAllocationType/offheap_buffers
                  "heap_buffers"
                  Config$MemtableAllocationType/heap_buffers
                  "offheap_objects"
                  Config$MemtableAllocationType/offheap_objects}]
    (set-field! config key (get ma-types value))))

(defmethod apply-field :internode_compression
  [config key value]
  (let [inc-types {"all"  Config$InternodeCompression/all
                   "dc"   Config$InternodeCompression/dc
                   "none" Config$InternodeCompression/none}]
    (set-field! config key (get inc-types value))))

(defmethod apply-field :seed_provider
  [config key value]
  (let [lhm (LinkedHashMap. value)
        sp  (SeedProviderDef. lhm)]
    (set-field! config key sp)))

(defmethod apply-field :default
  [config key value]
  (set-field! config key value))

(defn -loadConfig
  [this]
  (let [params (merge default-config (:cassandra (config/load-path nil)))
        config (Config.)]
    (debug "apply cluster configuration")
    (doseq [[k v] params]
      (debug "setting " k v)
      (apply-field config k v))
    (debug "yielding cluster with name: " (.cluster_name config))
    config))
