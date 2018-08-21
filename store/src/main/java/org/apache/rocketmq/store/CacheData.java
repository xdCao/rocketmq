package org.apache.rocketmq.store;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
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

    private final CopyOnWriteArrayList<MessageExtBrokerInner> dataList = new CopyOnWriteArrayList<>();

    private final MemoryMessageStore memoryMessageStore;

    private final HashMap<String/* topic-queueid */, List<Integer>/* index */> topicTable = new HashMap<String, List<Integer>>(1024);

    private final PutMessageLock putMessageLock;

    private AtomicInteger offset = new AtomicInteger(0);


    public CacheData(final MemoryMessageStore memoryMessageStore) {
        this.memoryMessageStore = memoryMessageStore;
        this.putMessageLock = memoryMessageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();
    }


    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
        msg.setStoreTimestamp(System.currentTimeMillis());
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));

        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.memoryMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
                || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery 不支持延时消息,因为延时消息和defaultMessageStore高耦合

        }

        long eclipseTimeInLock = 0;

        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            long beginLockTimestamp = this.memoryMessageStore.getSystemClock().now();

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            msg.setStoreTimestamp(beginLockTimestamp);

            dataList.add(msg);

            List<Integer> integers = topicTable.get(msg.getTopic());
            if (integers == null) {
                topicTable.put(msg.getTopic(), new ArrayList<Integer>());
                integers = topicTable.get(msg.getTopic());
            }

            integers.add(offset.getAndIncrement());

            eclipseTimeInLock = this.memoryMessageStore.getSystemClock().now() - beginLockTimestamp;

        } finally {
            putMessageLock.unlock();
        }

        if (eclipseTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipseTimeInLock, msg.getBody().length, result);
        }

        result = new AppendMessageResult(AppendMessageStatus.PUT_OK, 0, msg.getBody() == null ? 0 : msg.getBody().length, msg.getMsgId(), msg.getStoreTimestamp(), 0, 0);

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
        storeStatsService.getSinglePutMessageTopicSizeTotal(msg.getTopic()).addAndGet(result.getWroteBytes());

        /*暂时先不支持事务*/
//        handleHA(result, putMessageResult, msg);

        return putMessageResult;
    }


    public long getMaxOffset(String topic) {

        List<Integer> indexes = topicTable.get(topic);
        if (indexes == null || indexes.size() == 0) {
            return 0L;
        }

        return indexes.get(indexes.size() - 1);

    }


    public long getMinOffset(String topic) {

        List<Integer> indexes = topicTable.get(topic);
        if (indexes == null || indexes.size() == 0) {
            return 0L;
        }

        return indexes.get(0);

    }


    public long getCommitLogOffsetInQueue(String topic, long offset) {
        List<Integer> indexes = topicTable.get(topic);
        if (indexes == null || indexes.size() == 0 || offset >= indexes.size()) {
            return 0L;
        }

        return indexes.get((int) offset);
    }

    public MessageExt lookMessageByOffset(long commitLogOffset) {

        if (commitLogOffset >= dataList.size()) {
            return null;
        }

        return dataList.get((int) commitLogOffset);
    }

    public SelectMappedBufferResult getCommitLogData(long offset) {

        MessageExt messageExt = lookMessageByOffset(offset);

        byte[] body = messageExt.getBody();

        if (body == null || body.length == 0) {
            return null;
        }

        SelectMappedBufferResult result = new SelectMappedBufferResult(offset, ByteBuffer.wrap(body), body.length, null);
        return result;
    }


    public List<SelectMappedBufferResult> getMessagesFromOffset(String topic, long offset, int maxMsgNums) {

        List<Integer> indexs = topicTable.get(topic);

        if (indexs == null || indexs.size() == 0) {
            return null;
        }

        int numCount = 0;

        List<SelectMappedBufferResult> results = new ArrayList<>();

        for (int i = 0; i < indexs.size(); i++) {

            if (numCount >= maxMsgNums) {
                break;
            }

            if (indexs.get(i) < offset) {
                continue;
            }

            MessageExtBrokerInner message = dataList.get(indexs.get(i));
            SelectMappedBufferResult result = new SelectMappedBufferResult(i, message.getBody() == null ? null : ByteBuffer.wrap(message.getBody()), 0, null);
            results.add(result);
            numCount++;

        }

        return results;

    }
}
