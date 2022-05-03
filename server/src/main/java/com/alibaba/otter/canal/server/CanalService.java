package com.alibaba.otter.canal.server;

import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.server.exception.CanalServerException;

public interface CanalService {
    // 主要用于处理客户端的订阅请求，目前情况下，一个 CanalInstance 只能由一个客户端订阅，不过可以重复订阅。
    void subscribe(ClientIdentity clientIdentity) throws CanalServerException;
    // 主要用于取消订阅关系
    void unsubscribe(ClientIdentity clientIdentity) throws CanalServerException;
    // 比例获取数据，并自动自行 ack
    Message get(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;
    // 超时时间内批量获取数据，并自动进行 ack
    Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit) throws CanalServerException;
    // 批量获取数据，不进行 ack。用于客户端获取binlog消息 ，一个获取一批(batch)的 binlog，canal 会为这批 binlog 生成一个唯一的 batchId。客户端如果消费成功，则调用 ack 方法对这个批次进行确认。如果失败的话，可以调用 rollback 方法进行回滚。客户端可以连续多次调用 getWithoutAck 方法来获取 binlog，在 ack 的时候，需要按照获取到 binlog 的先后顺序进行 ack。如果后面获取的 binlog 被 ack 了，那么之前没有 ack 的 binlog 消息也会自动被 ack。
    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize) throws CanalServerException;
    // 超时时间内批量获取数据，不进行 ack
    Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit)
                                                                                                    throws CanalServerException;
    // ack 某个批次的数据
    void ack(ClientIdentity clientIdentity, long batchId) throws CanalServerException;
    // 回滚所有没有 ack 的批次的数据
    void rollback(ClientIdentity clientIdentity) throws CanalServerException;
    // 回滚某个批次的数据
    void rollback(ClientIdentity clientIdentity, Long batchId) throws CanalServerException;
}
