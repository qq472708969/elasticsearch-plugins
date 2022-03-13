package com.cgroup.synclinkrequest.loadsegment;

/**
 * Created by zzq on 2022/3/13.
 */
public interface Contents {
    String content = "{\n" +
            "  \"mappings\" : {\n" +
            "      \"doc\" : {\n" +
            "        \"dynamic\" : \"false\",\n" +
            "        \"date_detection\" : false,\n" +
            "        \"properties\" : {\n" +
            "          \"ccr_auto_follow_stats\" : {\n" +
            "            \"properties\" : {\n" +
            "              \"auto_followed_clusters\" : {\n" +
            "                \"type\" : \"nested\",\n" +
            "                \"properties\" : {\n" +
            "                  \"cluster_name\" : {\n" +
            "                    \"type\" : \"keyword\"\n" +
            "                  },\n" +
            "                  \"last_seen_metadata_version\" : {\n" +
            "                    \"type\" : \"long\"\n" +
            "                  },\n" +
            "                  \"time_since_last_check_millis\" : {\n" +
            "                    \"type\" : \"long\"\n" +
            "                  }\n" +
            "                }\n" +
            "              },\n" +
            "              \"number_of_failed_follow_indices\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"number_of_failed_remote_cluster_state_requests\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"number_of_successful_follow_indices\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"recent_auto_follow_errors\" : {\n" +
            "                \"type\" : \"nested\",\n" +
            "                \"properties\" : {\n" +
            "                  \"auto_follow_exception\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"reason\" : {\n" +
            "                        \"type\" : \"text\"\n" +
            "                      },\n" +
            "                      \"type\" : {\n" +
            "                        \"type\" : \"keyword\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"leader_index\" : {\n" +
            "                    \"type\" : \"keyword\"\n" +
            "                  },\n" +
            "                  \"timestamp\" : {\n" +
            "                    \"type\" : \"long\"\n" +
            "                  }\n" +
            "                }\n" +
            "              }\n" +
            "            }\n" +
            "          },\n" +
            "          \"ccr_stats\" : {\n" +
            "            \"properties\" : {\n" +
            "              \"bytes_read\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"failed_read_requests\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"failed_write_requests\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"fatal_exception\" : {\n" +
            "                \"properties\" : {\n" +
            "                  \"reason\" : {\n" +
            "                    \"type\" : \"text\"\n" +
            "                  },\n" +
            "                  \"type\" : {\n" +
            "                    \"type\" : \"keyword\"\n" +
            "                  }\n" +
            "                }\n" +
            "              },\n" +
            "              \"follower_global_checkpoint\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"follower_index\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"follower_mapping_version\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"follower_max_seq_no\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"follower_settings_version\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"last_requested_seq_no\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"leader_global_checkpoint\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"leader_index\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"leader_max_seq_no\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"operations_read\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"operations_written\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"outstanding_read_requests\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"outstanding_write_requests\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"read_exceptions\" : {\n" +
            "                \"type\" : \"nested\",\n" +
            "                \"properties\" : {\n" +
            "                  \"exception\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"reason\" : {\n" +
            "                        \"type\" : \"text\"\n" +
            "                      },\n" +
            "                      \"type\" : {\n" +
            "                        \"type\" : \"keyword\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"from_seq_no\" : {\n" +
            "                    \"type\" : \"long\"\n" +
            "                  },\n" +
            "                  \"retries\" : {\n" +
            "                    \"type\" : \"integer\"\n" +
            "                  }\n" +
            "                }\n" +
            "              },\n" +
            "              \"remote_cluster\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"shard_id\" : {\n" +
            "                \"type\" : \"integer\"\n" +
            "              },\n" +
            "              \"successful_read_requests\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"successful_write_requests\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"time_since_last_read_millis\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"total_read_remote_exec_time_millis\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"total_read_time_millis\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"total_write_time_millis\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"write_buffer_operation_count\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"write_buffer_size_in_bytes\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              }\n" +
            "            }\n" +
            "          },\n" +
            "          \"cluster_state\" : {\n" +
            "            \"properties\" : {\n" +
            "              \"master_node\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"nodes\" : {\n" +
            "                \"type\" : \"object\"\n" +
            "              },\n" +
            "              \"nodes_hash\" : {\n" +
            "                \"type\" : \"integer\"\n" +
            "              },\n" +
            "              \"shards\" : {\n" +
            "                \"type\" : \"object\"\n" +
            "              },\n" +
            "              \"state_uuid\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"status\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"version\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              }\n" +
            "            }\n" +
            "          },\n" +
            "          \"cluster_stats\" : {\n" +
            "            \"properties\" : {\n" +
            "              \"indices\" : {\n" +
            "                \"type\" : \"object\"\n" +
            "              },\n" +
            "              \"nodes\" : {\n" +
            "                \"type\" : \"object\"\n" +
            "              }\n" +
            "            }\n" +
            "          },\n" +
            "          \"cluster_uuid\" : {\n" +
            "            \"type\" : \"keyword\"\n" +
            "          },\n" +
            "          \"index_recovery\" : {\n" +
            "            \"type\" : \"object\"\n" +
            "          },\n" +
            "          \"index_stats\" : {\n" +
            "            \"properties\" : {\n" +
            "              \"index\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"primaries\" : {\n" +
            "                \"properties\" : {\n" +
            "                  \"docs\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"count\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"fielddata\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"evictions\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"memory_size_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"indexing\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"index_time_in_millis\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"index_total\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"throttle_time_in_millis\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"merges\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"total_size_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"query_cache\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"evictions\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"hit_count\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"memory_size_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"miss_count\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"refresh\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"total_time_in_millis\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"request_cache\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"evictions\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"hit_count\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"memory_size_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"miss_count\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"search\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"query_time_in_millis\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"query_total\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"segments\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"count\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      },\n" +
            "                      \"doc_values_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"fixed_bit_set_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"index_writer_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"norms_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"points_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"stored_fields_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"term_vectors_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"terms_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"version_map_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"store\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"size_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  }\n" +
            "                }\n" +
            "              },\n" +
            "              \"total\" : {\n" +
            "                \"properties\" : {\n" +
            "                  \"docs\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"count\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"fielddata\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"evictions\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"memory_size_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"indexing\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"index_time_in_millis\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"index_total\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"throttle_time_in_millis\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"merges\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"total_size_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"query_cache\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"evictions\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"hit_count\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"memory_size_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"miss_count\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"refresh\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"total_time_in_millis\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"request_cache\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"evictions\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"hit_count\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"memory_size_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"miss_count\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"search\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"query_time_in_millis\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"query_total\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"segments\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"count\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      },\n" +
            "                      \"doc_values_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"fixed_bit_set_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"index_writer_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"norms_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"points_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"stored_fields_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"term_vectors_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"terms_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"version_map_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"store\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"size_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  }\n" +
            "                }\n" +
            "              }\n" +
            "            }\n" +
            "          },\n" +
            "          \"indices_stats\" : {\n" +
            "            \"properties\" : {\n" +
            "              \"_all\" : {\n" +
            "                \"properties\" : {\n" +
            "                  \"primaries\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"docs\" : {\n" +
            "                        \"properties\" : {\n" +
            "                          \"count\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          }\n" +
            "                        }\n" +
            "                      },\n" +
            "                      \"indexing\" : {\n" +
            "                        \"properties\" : {\n" +
            "                          \"index_time_in_millis\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          },\n" +
            "                          \"index_total\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          }\n" +
            "                        }\n" +
            "                      },\n" +
            "                      \"search\" : {\n" +
            "                        \"properties\" : {\n" +
            "                          \"query_time_in_millis\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          },\n" +
            "                          \"query_total\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          }\n" +
            "                        }\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"total\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"docs\" : {\n" +
            "                        \"properties\" : {\n" +
            "                          \"count\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          }\n" +
            "                        }\n" +
            "                      },\n" +
            "                      \"indexing\" : {\n" +
            "                        \"properties\" : {\n" +
            "                          \"index_time_in_millis\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          },\n" +
            "                          \"index_total\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          }\n" +
            "                        }\n" +
            "                      },\n" +
            "                      \"search\" : {\n" +
            "                        \"properties\" : {\n" +
            "                          \"query_time_in_millis\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          },\n" +
            "                          \"query_total\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          }\n" +
            "                        }\n" +
            "                      }\n" +
            "                    }\n" +
            "                  }\n" +
            "                }\n" +
            "              }\n" +
            "            }\n" +
            "          },\n" +
            "          \"interval_ms\" : {\n" +
            "            \"type\" : \"long\"\n" +
            "          },\n" +
            "          \"job_stats\" : {\n" +
            "            \"properties\" : {\n" +
            "              \"data_counts\" : {\n" +
            "                \"properties\" : {\n" +
            "                  \"bucket_count\" : {\n" +
            "                    \"type\" : \"long\"\n" +
            "                  },\n" +
            "                  \"earliest_record_timestamp\" : {\n" +
            "                    \"type\" : \"date\"\n" +
            "                  },\n" +
            "                  \"empty_bucket_count\" : {\n" +
            "                    \"type\" : \"long\"\n" +
            "                  },\n" +
            "                  \"input_bytes\" : {\n" +
            "                    \"type\" : \"long\"\n" +
            "                  },\n" +
            "                  \"latest_record_timestamp\" : {\n" +
            "                    \"type\" : \"date\"\n" +
            "                  },\n" +
            "                  \"processed_record_count\" : {\n" +
            "                    \"type\" : \"long\"\n" +
            "                  },\n" +
            "                  \"sparse_bucket_count\" : {\n" +
            "                    \"type\" : \"long\"\n" +
            "                  }\n" +
            "                }\n" +
            "              },\n" +
            "              \"job_id\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"model_size_stats\" : {\n" +
            "                \"properties\" : {\n" +
            "                  \"bucket_allocation_failures_count\" : {\n" +
            "                    \"type\" : \"long\"\n" +
            "                  },\n" +
            "                  \"model_bytes\" : {\n" +
            "                    \"type\" : \"long\"\n" +
            "                  }\n" +
            "                }\n" +
            "              },\n" +
            "              \"node\" : {\n" +
            "                \"properties\" : {\n" +
            "                  \"id\" : {\n" +
            "                    \"type\" : \"keyword\"\n" +
            "                  }\n" +
            "                }\n" +
            "              },\n" +
            "              \"state\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              }\n" +
            "            }\n" +
            "          },\n" +
            "          \"node_stats\" : {\n" +
            "            \"properties\" : {\n" +
            "              \"fs\" : {\n" +
            "                \"properties\" : {\n" +
            "                  \"data\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"spins\" : {\n" +
            "                        \"type\" : \"boolean\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"io_stats\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"total\" : {\n" +
            "                        \"properties\" : {\n" +
            "                          \"operations\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          },\n" +
            "                          \"read_kilobytes\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          },\n" +
            "                          \"read_operations\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          },\n" +
            "                          \"write_kilobytes\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          },\n" +
            "                          \"write_operations\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          }\n" +
            "                        }\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"total\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"available_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"free_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"total_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  }\n" +
            "                }\n" +
            "              },\n" +
            "              \"indices\" : {\n" +
            "                \"properties\" : {\n" +
            "                  \"docs\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"count\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"fielddata\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"evictions\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"memory_size_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"indexing\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"index_time_in_millis\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"index_total\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"throttle_time_in_millis\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"query_cache\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"evictions\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"hit_count\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"memory_size_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"miss_count\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"request_cache\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"evictions\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"hit_count\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"memory_size_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"miss_count\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"search\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"query_time_in_millis\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"query_total\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"segments\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"count\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      },\n" +
            "                      \"doc_values_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"fixed_bit_set_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"index_writer_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"norms_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"points_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"stored_fields_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"term_vectors_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"terms_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"version_map_memory_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"store\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"size_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  }\n" +
            "                }\n" +
            "              },\n" +
            "              \"jvm\" : {\n" +
            "                \"properties\" : {\n" +
            "                  \"gc\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"collectors\" : {\n" +
            "                        \"properties\" : {\n" +
            "                          \"old\" : {\n" +
            "                            \"properties\" : {\n" +
            "                              \"collection_count\" : {\n" +
            "                                \"type\" : \"long\"\n" +
            "                              },\n" +
            "                              \"collection_time_in_millis\" : {\n" +
            "                                \"type\" : \"long\"\n" +
            "                              }\n" +
            "                            }\n" +
            "                          },\n" +
            "                          \"young\" : {\n" +
            "                            \"properties\" : {\n" +
            "                              \"collection_count\" : {\n" +
            "                                \"type\" : \"long\"\n" +
            "                              },\n" +
            "                              \"collection_time_in_millis\" : {\n" +
            "                                \"type\" : \"long\"\n" +
            "                              }\n" +
            "                            }\n" +
            "                          }\n" +
            "                        }\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"mem\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"heap_max_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"heap_used_in_bytes\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"heap_used_percent\" : {\n" +
            "                        \"type\" : \"half_float\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  }\n" +
            "                }\n" +
            "              },\n" +
            "              \"mlockall\" : {\n" +
            "                \"type\" : \"boolean\"\n" +
            "              },\n" +
            "              \"node_id\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"node_master\" : {\n" +
            "                \"type\" : \"boolean\"\n" +
            "              },\n" +
            "              \"os\" : {\n" +
            "                \"properties\" : {\n" +
            "                  \"cgroup\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"cpu\" : {\n" +
            "                        \"properties\" : {\n" +
            "                          \"cfs_quota_micros\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          },\n" +
            "                          \"control_group\" : {\n" +
            "                            \"type\" : \"keyword\"\n" +
            "                          },\n" +
            "                          \"stat\" : {\n" +
            "                            \"properties\" : {\n" +
            "                              \"number_of_elapsed_periods\" : {\n" +
            "                                \"type\" : \"long\"\n" +
            "                              },\n" +
            "                              \"number_of_times_throttled\" : {\n" +
            "                                \"type\" : \"long\"\n" +
            "                              },\n" +
            "                              \"time_throttled_nanos\" : {\n" +
            "                                \"type\" : \"long\"\n" +
            "                              }\n" +
            "                            }\n" +
            "                          }\n" +
            "                        }\n" +
            "                      },\n" +
            "                      \"cpuacct\" : {\n" +
            "                        \"properties\" : {\n" +
            "                          \"control_group\" : {\n" +
            "                            \"type\" : \"keyword\"\n" +
            "                          },\n" +
            "                          \"usage_nanos\" : {\n" +
            "                            \"type\" : \"long\"\n" +
            "                          }\n" +
            "                        }\n" +
            "                      },\n" +
            "                      \"memory\" : {\n" +
            "                        \"properties\" : {\n" +
            "                          \"control_group\" : {\n" +
            "                            \"type\" : \"keyword\"\n" +
            "                          },\n" +
            "                          \"limit_in_bytes\" : {\n" +
            "                            \"type\" : \"keyword\"\n" +
            "                          },\n" +
            "                          \"usage_in_bytes\" : {\n" +
            "                            \"type\" : \"keyword\"\n" +
            "                          }\n" +
            "                        }\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"cpu\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"load_average\" : {\n" +
            "                        \"properties\" : {\n" +
            "                          \"15m\" : {\n" +
            "                            \"type\" : \"half_float\"\n" +
            "                          },\n" +
            "                          \"1m\" : {\n" +
            "                            \"type\" : \"half_float\"\n" +
            "                          },\n" +
            "                          \"5m\" : {\n" +
            "                            \"type\" : \"half_float\"\n" +
            "                          }\n" +
            "                        }\n" +
            "                      }\n" +
            "                    }\n" +
            "                  }\n" +
            "                }\n" +
            "              },\n" +
            "              \"process\" : {\n" +
            "                \"properties\" : {\n" +
            "                  \"cpu\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"percent\" : {\n" +
            "                        \"type\" : \"half_float\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"max_file_descriptors\" : {\n" +
            "                    \"type\" : \"long\"\n" +
            "                  },\n" +
            "                  \"open_file_descriptors\" : {\n" +
            "                    \"type\" : \"long\"\n" +
            "                  }\n" +
            "                }\n" +
            "              },\n" +
            "              \"thread_pool\" : {\n" +
            "                \"properties\" : {\n" +
            "                  \"bulk\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"queue\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      },\n" +
            "                      \"rejected\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"threads\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"generic\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"queue\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      },\n" +
            "                      \"rejected\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"threads\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"get\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"queue\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      },\n" +
            "                      \"rejected\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"threads\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"index\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"queue\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      },\n" +
            "                      \"rejected\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"threads\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"management\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"queue\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      },\n" +
            "                      \"rejected\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"threads\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"search\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"queue\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      },\n" +
            "                      \"rejected\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"threads\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"watcher\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"queue\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      },\n" +
            "                      \"rejected\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      },\n" +
            "                      \"threads\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  },\n" +
            "                  \"write\" : {\n" +
            "                    \"properties\" : {\n" +
            "                      \"queue\" : {\n" +
            "                        \"type\" : \"integer\"\n" +
            "                      },\n" +
            "                      \"rejected\" : {\n" +
            "                        \"type\" : \"long\"\n" +
            "                      }\n" +
            "                    }\n" +
            "                  }\n" +
            "                }\n" +
            "              }\n" +
            "            }\n" +
            "          },\n" +
            "          \"shard\" : {\n" +
            "            \"properties\" : {\n" +
            "              \"index\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"node\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"primary\" : {\n" +
            "                \"type\" : \"boolean\"\n" +
            "              },\n" +
            "              \"relocating_node\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"shard\" : {\n" +
            "                \"type\" : \"long\"\n" +
            "              },\n" +
            "              \"state\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              }\n" +
            "            }\n" +
            "          },\n" +
            "          \"source_node\" : {\n" +
            "            \"properties\" : {\n" +
            "              \"host\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"ip\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"name\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"timestamp\" : {\n" +
            "                \"type\" : \"date\",\n" +
            "                \"format\" : \"date_time\"\n" +
            "              },\n" +
            "              \"transport_address\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              },\n" +
            "              \"uuid\" : {\n" +
            "                \"type\" : \"keyword\"\n" +
            "              }\n" +
            "            }\n" +
            "          },\n" +
            "          \"state_uuid\" : {\n" +
            "            \"type\" : \"keyword\"\n" +
            "          },\n" +
            "          \"timestamp\" : {\n" +
            "            \"type\" : \"date\",\n" +
            "            \"format\" : \"date_time\"\n" +
            "          },\n" +
            "          \"type\" : {\n" +
            "            \"type\" : \"keyword\"\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "  \"settings\" : {\n" +
            "      \"index\" : {\n" +
            "        \"codec\" : \"best_compression\",\n" +
            "        \"routing\" : {\n" +
            "          \"allocation\" : {\n" +
            "            \"require\" : {\n" +
            "              \"group_type\" : \"g2\"\n" +
            "            }\n" +
            "          }\n" +
            "        },\n" +
            "        \"number_of_shards\" : \"2\",\n" +
            "        \"auto_expand_replicas\" : \"false\",\n" +
            "        \"format\" : \"6\",\n" +
            "        \"number_of_replicas\" : \"0\"\n" +
            "      }\n" +
            "    }\n" +
            "}";
}
