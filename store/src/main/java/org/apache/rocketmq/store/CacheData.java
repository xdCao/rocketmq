package org.apache.rocketmq.store;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: rocketmq-all
 * @description: 内存数据缓存
 * @author: xdCao
 * @create: 2018-08-20 18:26
 **/
public class CacheData {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final ThreadLocal<CommitLog.MessageExtBatchEncoder> batchEncoderThreadLocal;


    private final ConcurrentHashMap<String/*topic*/, MemoryDataWithIndex> dataMap = new ConcurrentHashMap<>();



    private final MemoryMessageStore memoryMessageStore;

    private HashMap<String/* topic-queueid */, Long/* index */> topicQueueTable = new HashMap<String, Long>(1024);

    private final PutMessageLock putMessageLock;

    private volatile long beginTimeInLock = 0;





    public CacheData(final MemoryMessageStore memoryMessageStore) {
        this.memoryMessageStore = memoryMessageStore;
        this.putMessageLock = memoryMessageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();
        batchEncoderThreadLocal = new ThreadLocal<CommitLog.MessageExtBatchEncoder>() {
            @Override
            protected CommitLog.MessageExtBatchEncoder initialValue() {
                return new CommitLog.MessageExtBatchEncoder(memoryMessageStore.getMessageStoreConfig().getMaxMessageSize());
            }
        };
    }


    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
        msg.setStoreTimestamp(System.currentTimeMillis());
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));

        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.memoryMessageStore.getStoreStatsService();

        String topic = msg.getTopic();
        int queueId = msg.getQueueId();

        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
                || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery 不支持延时消息,因为延时消息和defaultMessageStore高耦合

        }

        long eclipseTimeInLock = 0;

        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            long beginLockTimestamp = this.memoryMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            msg.setStoreTimestamp(beginLockTimestamp);

            MemoryDataWithIndex memoryDataWithIndex = dataMap.get(msg.getTopic());

            if (memoryDataWithIndex == null) {
                dataMap.put(msg.getTopic(),new MemoryDataWithIndex());
                memoryDataWithIndex = dataMap.get(msg.getTopic());
            }

            memoryDataWithIndex.putMessage(msg);


            eclipseTimeInLock = this.memoryMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (eclipseTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipseTimeInLock, msg.getBody().length, result);
        }

        result = new AppendMessageResult(AppendMessageStatus.PUT_OK, 0, msg.getBody()==null?0:msg.getBody().length, msg.getMsgId(), msg.getStoreTimestamp(), 0, 0);

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());

        /*暂时先不支持事务*/
//        handleHA(result, putMessageResult, msg);

        return putMessageResult;
    }


    public MessageExtBrokerInner getMessage(String topic, int offset) {

        if (dataMap.get(topic) == null) {
            return null;
        }

        return dataMap.get(topic).getMessage(offset);

    }


    public long getMaxOffset(String topic) {

        return dataMap.get(topic)==null?0:dataMap.get(topic).getNextIndex().get()-1;

    }

    public List<SelectMappedBufferResult> getMessagesFromOffset(String topic, long offset, int maxMsgNums) {

        MemoryDataWithIndex dataWithIndex = dataMap.get(topic);
        if (dataWithIndex == null || offset >= dataWithIndex.getNextIndex().get()) {
            return null;
        }

        List<SelectMappedBufferResult> results = new ArrayList<>();



        for (long i = offset; i < offset+maxMsgNums; i++) {
            if (i >= dataWithIndex.getNextIndex().get()) {
                break;
            }

            MessageExtBrokerInner message = dataWithIndex.getMessage((int) i);
            SelectMappedBufferResult result = new SelectMappedBufferResult(i,message.getBody()==null?null: ByteBuffer.wrap(message.getBody()),0,null);
            results.add(result);

        }

        return results;



    }
}
