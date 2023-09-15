/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2023, Confluent Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "../src/rdkafka_proto.h"
#include "test.h"
#include "../src/rdkafka_mock.h"
/**
 * @name Verify that the builtin mock cluster works by producing to a topic
 *       and then consuming from it.
 */
const int32_t retry_ms = 100;
const int32_t retry_max_ms = 1000; 

/** Find Coordinator Test
 * We fail the request with RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE so that
 * the request is retried via the intervalled mechanism and it should be close to 1 or 2 seconds
 * as the loop runs after every 1 second and sends the request which have been there for atleast 500 ms
 * The exponential backoff does not apply in this case we just apply the jitter to the backoff of intervalled query
*/
void test_FindCoordinator(rd_kafka_mock_cluster_t *mcluster, const char *topic, rd_kafka_conf_t *conf){
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;
        int64_t previous_request_ts = -1;
        int32_t retry_count = 0;
        int32_t num_retries = 4;
        const int32_t low = 1000;
        const int32_t high = 2000;
        int32_t buffer = 200; // 200 ms buffer added
        rd_kafka_t *consumer;

        SUB_TEST();
        test_conf_set(conf, "topic.metadata.refresh.interval.ms","-1");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");

        consumer = test_create_consumer(topic, NULL, rd_kafka_conf_dup(conf), NULL);
        
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_FindCoordinator, num_retries,
                                          RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE,
                                          RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE,
                                          RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE,
                                          RD_KAFKA_RESP_ERR_GROUP_COORDINATOR_NOT_AVAILABLE);
        
        rd_kafka_message_t *rkm = rd_kafka_consumer_poll(consumer,10*1000);
        if (rkm)
                rd_kafka_message_destroy(rkm);
        rd_sleep(4);
        requests = rd_kafka_mock_get_requests(mcluster,&request_cnt);
        for(size_t i = 0; (i < request_cnt) && (retry_count < num_retries); i++){
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %lld\n",rd_kafka_mock_request_id(requests[i]), rd_kafka_mock_request_api_key(requests[i]), rd_kafka_mock_request_timestamp(requests[i]));
                if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_FindCoordinator){
                        if(previous_request_ts != -1){
                                int64_t time_difference = (rd_kafka_mock_request_timestamp(requests[i]) - previous_request_ts)/1000;
                                TEST_ASSERT(((time_difference > high - buffer) && (time_difference < high + buffer)) || ((time_difference > low - buffer) && (time_difference < low + buffer)) ,"Time difference should be close to 1 or 2 seconds, it is %lld ms instead.\n",time_difference);
                                retry_count++;
                        }
                        previous_request_ts = rd_kafka_mock_request_timestamp(requests[i]);
                }
        }
        rd_kafka_destroy(consumer);
        rd_kafka_mock_clear_requests(mcluster);
        SUB_TEST_PASS();
}

/** OffsetCommit Test
 * We fail the request with RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS so that
 * the request is retried with the exponential backoff. The max retries allowed is 2.
*/
void test_OffsetCommit(rd_kafka_mock_cluster_t *mcluster,const char *topic, rd_kafka_conf_t *conf){
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;
        int64_t previous_request_ts = -1;
        int32_t retry_count = 0;
        rd_kafka_t *consumer;

        SUB_TEST();
        test_conf_set(conf, "topic.metadata.refresh.interval.ms","-1");
        test_conf_set(conf, "debug" ,"generic,cgrp");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");

        consumer = test_create_consumer(topic, NULL, rd_kafka_conf_dup(conf), NULL);
        test_consumer_subscribe(consumer,topic);
        rd_kafka_message_t *rkm = rd_kafka_consumer_poll(consumer,10*1000);
        if (rkm)
                rd_kafka_message_destroy(rkm);
        rd_sleep(4);
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_OffsetCommit, 2,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS);

        rd_kafka_topic_partition_list_t *offsets = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_t *rktpar = rd_kafka_topic_partition_list_add(offsets,topic,0);
        rktpar->offset = 4;
        rd_kafka_commit(consumer,offsets,0);
        rd_kafka_topic_partition_list_destroy(offsets);
        rd_sleep(3);
        requests = rd_kafka_mock_get_requests(mcluster,&request_cnt);
        for(size_t i = 0; i < request_cnt; i++){
                if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_OffsetCommit){
                        TEST_SAY("Broker Id : %d API Key : %d Timestamp : %lld\n",rd_kafka_mock_request_id(requests[i]), rd_kafka_mock_request_api_key(requests[i]), rd_kafka_mock_request_timestamp(requests[i]));
                        if(previous_request_ts != -1){
                                int64_t time_difference = (rd_kafka_mock_request_timestamp(requests[i]) - previous_request_ts)/1000;
                                int64_t low = ((1<<retry_count)*(retry_ms)*75)/100; 
                                int64_t high = ((1<<retry_count)*(retry_ms)*125)/100;
                                if (high > ((retry_max_ms*125)/100))
                                        high = (retry_max_ms*125)/100;
                                if (low > ((retry_max_ms*75)/100))
                                        low = (retry_max_ms*75)/100;
                                TEST_ASSERT((time_difference < high) && (time_difference > low),"Time difference is not respected!\n");
                                retry_count++;
                        }
                        previous_request_ts = rd_kafka_mock_request_timestamp(requests[i]);
                }        
        }
        rd_kafka_destroy(consumer);
        rd_kafka_mock_clear_requests(mcluster);
        SUB_TEST_PASS();
}

/** Produce Test
 * We fail the request with RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS so that
 * the request is retried with the exponential backoff. The exponential backoff is capped at retry_max_ms with jitter.
*/
void test_Produce(rd_kafka_mock_cluster_t *mcluster,const char *topic, rd_kafka_conf_t *conf){
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;
        int64_t previous_request_ts = -1;
        int32_t retry_count = 0;
        rd_kafka_t *producer;
        rd_kafka_topic_t *rkt;
        SUB_TEST();
        test_conf_set(conf, "topic.metadata.refresh.interval.ms","-1");
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        producer = test_create_handle(RD_KAFKA_PRODUCER, rd_kafka_conf_dup(conf));
        rkt = test_create_producer_topic(producer, topic, NULL);

        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_Produce,  7,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS,
                                            RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS);
        
        test_produce_msgs(producer, rkt, 0, RD_KAFKA_PARTITION_UA, 0, 1, "hello",5);
        rd_sleep(3);

        requests = rd_kafka_mock_get_requests(mcluster,&request_cnt);
        for(size_t i = 0; i < request_cnt; i++){
                if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Produce){
                        TEST_SAY("Broker Id : %d API Key : %d Timestamp : %lld\n",rd_kafka_mock_request_id(requests[i]), rd_kafka_mock_request_api_key(requests[i]), rd_kafka_mock_request_timestamp(requests[i]));
                        if(previous_request_ts != -1){
                                int64_t time_difference = (rd_kafka_mock_request_timestamp(requests[i]) - previous_request_ts)/1000;
                                int64_t low = ((1<<retry_count)*(retry_ms)*75)/100; 
                                int64_t high = ((1<<retry_count)*(retry_ms)*125)/100;
                                if (high > ((retry_max_ms*125)/100))
                                        high = (retry_max_ms*125)/100;
                                if (low > ((retry_max_ms*75)/100))
                                        low = (retry_max_ms*75)/100;
                                TEST_ASSERT((time_difference < high) && (time_difference > low),"Time difference is not respected!\n");
                                retry_count++;
                        }
                        previous_request_ts = rd_kafka_mock_request_timestamp(requests[i]);
                }
        }
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(producer);
        rd_kafka_mock_clear_requests(mcluster);
        SUB_TEST_PASS();
}
/** Heartbeat-FindCoordinator Test
 * We fail the request with RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP so that
 * the FindCoordinator request is trigerred.
*/
void test_Heartbeat_FindCoordinator(rd_kafka_mock_cluster_t *mcluster,const char *topic, rd_kafka_conf_t *conf){
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;
        int32_t num_heartbeat = 0;
        rd_kafka_t *consumer;
        SUB_TEST();
        test_conf_set(conf, "topic.metadata.refresh.interval.ms","-1");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");

        consumer = test_create_consumer(topic, NULL, rd_kafka_conf_dup(conf), NULL);
        
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_Heartbeat,  1,
                                            RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP);

        rd_kafka_mock_clear_requests(mcluster);
        test_consumer_subscribe(consumer,topic);
        rd_kafka_message_t *rkm = rd_kafka_consumer_poll(consumer,10*1000);
        if (rkm)
                rd_kafka_message_destroy(rkm);
        rd_sleep(6);
        requests = rd_kafka_mock_get_requests(mcluster,&request_cnt);
        for(size_t i = 0; i < request_cnt; i++){
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %lld\n",rd_kafka_mock_request_id(requests[i]), rd_kafka_mock_request_api_key(requests[i]), rd_kafka_mock_request_timestamp(requests[i]));
                if(num_heartbeat == 0){
                        if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Heartbeat){
                                num_heartbeat++;
                        }
                }else if(num_heartbeat == 1){
                        if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_FindCoordinator){
                                TEST_SAY("FindCoordinator request made after failing Heartbeat with NOT_COORDINATOR error.\n");
                                break;
                        }else if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Heartbeat){
                                num_heartbeat++;
                                TEST_FAIL("Second Heartbeat Request made without any FindCoordinator Request in-between.\n");
                        }
                }
        }
        TEST_ASSERT((num_heartbeat == 1),"No Heartbeat Request was made.\n");
        rd_kafka_destroy(consumer);
        rd_kafka_mock_clear_requests(mcluster);
        SUB_TEST_PASS();
}

/** Joingroup-FindCoordinator Test
 * We fail the request with RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP so that
 * the FindCoordinator request is trigerred.
*/
void test_JoinGroup_FindCoordinator(rd_kafka_mock_cluster_t *mcluster,const char *topic, rd_kafka_conf_t *conf){
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;
        int32_t num_joingroup = 0;
        rd_kafka_t *consumer;
        SUB_TEST();
        test_conf_set(conf, "topic.metadata.refresh.interval.ms","-1");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");

        consumer = test_create_consumer(topic, NULL, rd_kafka_conf_dup(conf), NULL);
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_JoinGroup, 1 ,
                                            RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_GROUP);
        rd_kafka_mock_clear_requests(mcluster);
        test_consumer_subscribe(consumer,topic);
        rd_kafka_message_t *rkm = rd_kafka_consumer_poll(consumer,10*1000);
        if (rkm)
                rd_kafka_message_destroy(rkm);
        rd_sleep(4);
        requests = rd_kafka_mock_get_requests(mcluster,&request_cnt);
        for(size_t i = 0; i < request_cnt; i++){
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %lld\n",rd_kafka_mock_request_id(requests[i]), rd_kafka_mock_request_api_key(requests[i]), rd_kafka_mock_request_timestamp(requests[i]));
                if(num_joingroup == 0){
                        if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_JoinGroup){
                                num_joingroup++;
                        }
                }else if(num_joingroup == 1){
                        if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_FindCoordinator){
                                TEST_SAY("FindCoordinator request made after failing JoinGroup with NOT_COORDINATOR error.\n");
                                break;
                        }else if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_JoinGroup){
                                num_joingroup++;
                                TEST_FAIL("Second JoinGroup Request made without any FindCoordinator Request in-between.\n");
                        }
                }
        }
        TEST_ASSERT((num_joingroup == 1),"No JoinGroup Request was made.\n");
        rd_kafka_destroy(consumer);
        rd_kafka_mock_clear_requests(mcluster);
        SUB_TEST_PASS();
}

/** Produce-FastLeaderQuery Test
 * We fail the request with RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER so that it triggers FastLeaderQuery request
 * which is backed off exponentially
*/
void test_Produce_FastleaderQuery(rd_kafka_mock_cluster_t *mcluster,const char *topic, rd_kafka_conf_t *conf){
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;
        int64_t previous_request_ts = -1;
        int32_t retry_count = 0;
        rd_bool_t produced = rd_false;
        SUB_TEST();
        test_conf_set(conf, "topic.metadata.refresh.interval.ms","-1");
        rd_kafka_conf_set_dr_msg_cb(conf, test_dr_msg_cb);

        rd_kafka_t *producer = test_create_handle(RD_KAFKA_PRODUCER, rd_kafka_conf_dup(conf));
        rd_kafka_topic_t *rkt = test_create_producer_topic(producer, topic, NULL);
        
        rd_kafka_mock_push_request_errors(mcluster, RD_KAFKAP_Produce,  1,
                                            RD_KAFKA_RESP_ERR_NOT_LEADER_OR_FOLLOWER);
        rd_kafka_mock_clear_requests(mcluster);
        test_produce_msgs(producer, rkt, 0, RD_KAFKA_PARTITION_UA, 0, 1, "hello",1);
        rd_sleep(10);
        requests = rd_kafka_mock_get_requests(mcluster,&request_cnt);
        
        for(size_t i = 0; i < request_cnt; i++){
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %lld\n",rd_kafka_mock_request_id(requests[i]), rd_kafka_mock_request_api_key(requests[i]), rd_kafka_mock_request_timestamp(requests[i]));
                if(!produced && rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Produce)
                        produced = rd_true;
                if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Metadata && produced){
                        if(previous_request_ts != -1){
                                int64_t time_difference = (rd_kafka_mock_request_timestamp(requests[i]) - previous_request_ts)/1000;
                                int64_t low = ((1<<retry_count)*(retry_ms)*75)/100; 
                                int64_t high = ((1<<retry_count)*(retry_ms)*125)/100;
                                if (high > ((retry_max_ms*125)/100))
                                        high = (retry_max_ms*125)/100;
                                if (low > ((retry_max_ms*75)/100))
                                        low = (retry_max_ms*75)/100;
                                TEST_ASSERT((time_difference < high) && (time_difference > low),"Time difference is not respected!\n");
                                retry_count++;
                        }
                        previous_request_ts = rd_kafka_mock_request_timestamp(requests[i]);
                }
        }
        rd_kafka_topic_destroy(rkt);
        rd_kafka_destroy(producer); 
        rd_kafka_mock_clear_requests(mcluster);
        SUB_TEST_PASS();
}

/** Fetch-FastLeaderQuery Test
 * We fail the request with leader change or epoch to be precise so that it triggers FastLeaderQuery request.
*/
void test_Fetch_FastLeaderQuery(rd_kafka_mock_cluster_t *mcluster,const char *topic, rd_kafka_conf_t *conf){
        rd_kafka_mock_request_t **requests = NULL;
        size_t request_cnt = 0;
        rd_bool_t fetched = rd_false;
        rd_bool_t flag = rd_false;
        rd_kafka_t *consumer;
        SUB_TEST();
        test_conf_set(conf, "topic.metadata.refresh.interval.ms","-1");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "enable.auto.commit", "false");

        consumer = test_create_consumer(topic, NULL, rd_kafka_conf_dup(conf), NULL);
        
        test_consumer_subscribe(consumer,topic);
        rd_kafka_message_t *rkm = rd_kafka_consumer_poll(consumer,10*1000);
        if (rkm)
                rd_kafka_message_destroy(rkm);
        rd_kafka_mock_clear_requests(mcluster);
        
        rd_kafka_mock_partition_set_leader(mcluster,topic,0,1);
        rkm = rd_kafka_consumer_poll(consumer,10*1000);
        if (rkm)
                rd_kafka_message_destroy(rkm);
        rd_sleep(3);
        requests = rd_kafka_mock_get_requests(mcluster,&request_cnt);
        for(size_t i = 0; i < request_cnt; i++){
                TEST_SAY("Broker Id : %d API Key : %d Timestamp : %lld\n",rd_kafka_mock_request_id(requests[i]), rd_kafka_mock_request_api_key(requests[i]), rd_kafka_mock_request_timestamp(requests[i]));
                if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Fetch)
                        fetched = rd_true;
                else if(rd_kafka_mock_request_api_key(requests[i]) == RD_KAFKAP_Metadata && fetched){
                        flag = rd_true;
                        break;
                }else{
                        fetched = rd_false;
                }
        }
        rd_kafka_destroy(consumer); 
        rd_kafka_mock_clear_requests(mcluster);
        TEST_ASSERT((flag==rd_true),"Metadata Request should have been made after fetch atleast once.\n");
        SUB_TEST_PASS();
}

/** Exponential Backoff (KIP 580)
 * We test all the pipelines which affect the retry mechanism for both intervalled queries where jitter is added and backed off
 * queries where both jitter and exponential backoff is applied with the max being retry_max_ms.
*/
int main_0143_exponential_backoff(int argc, char **argv) {
        const char *topic = test_mk_topic_name("topic", 1);
        rd_kafka_mock_cluster_t *mcluster;
        rd_kafka_conf_t *conf;
        const char *bootstraps;

        if (test_needs_auth()) {
                TEST_SKIP("Mock cluster does not support SSL/SASL\n");
                return 0;
        }

        mcluster = test_mock_cluster_new(1, &bootstraps);
        rd_kafka_mock_topic_create(mcluster, topic, 1, 1);

        test_conf_init(&conf, NULL, 30);
        test_conf_set(conf, "bootstrap.servers", bootstraps);
        
        test_Produce(mcluster,topic,rd_kafka_conf_dup(conf));
        test_Produce_FastleaderQuery(mcluster,topic,rd_kafka_conf_dup(conf)); 
        test_FindCoordinator(mcluster,topic,rd_kafka_conf_dup(conf));
        test_OffsetCommit(mcluster,topic,rd_kafka_conf_dup(conf));
        test_Heartbeat_FindCoordinator(mcluster,topic,rd_kafka_conf_dup(conf));
        test_JoinGroup_FindCoordinator(mcluster,topic,rd_kafka_conf_dup(conf));
        test_Fetch_FastLeaderQuery(mcluster,topic,rd_kafka_conf_dup(conf));

        test_mock_cluster_destroy(mcluster);
        return 0;
}
