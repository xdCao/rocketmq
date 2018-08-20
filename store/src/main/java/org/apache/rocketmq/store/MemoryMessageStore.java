package org.apache.rocketmq.store;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.rocketmq.store.config.BrokerRole.SLAVE;

/**
 * @program: rocketmq-all
 * @description: 不带持久化的store实现
 * @author: xdCao
 * @create: 2018-08-20 18:24
 **/
public class MemoryMessageStore implements MessageStore {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final MessageStoreConfig messageStoreConfig;

    private final CacheData cacheData;

    private final SystemClock systemClock = new SystemClock();

    private final StoreStatsService storeStatsService;

    private volatile boolean shutdown = true;

    private final BrokerConfig brokerConfig;

    private final BrokerStatsManager brokerStatsManager;

    private RandomAccessFile lockFile;

    private FileLock lock;

    private final MessageArrivingListener messageArrivingListener;

    private AtomicLong printTimes = new AtomicLong(0);

    boolean shutDownNormal = false;


    private final RunningFlags runningFlags = new RunningFlags();


    public MemoryMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
                              final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws FileNotFoundException {
        this.messageStoreConfig = messageStoreConfig;
        this.cacheData = new CacheData(this);
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
        this.brokerStatsManager = brokerStatsManager;
        this.storeStatsService = new StoreStatsService();


        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        MappedFile.ensureDirOK(file.getParent());
        lockFile = new RandomAccessFile(file, "rw");

    }

    @Override
    public boolean load() {
        return true;
    }


    @Override
    public void start() throws Exception {
        lock = lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
            throw new RuntimeException("Lock failed,MQ already started");
        }

        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
        lockFile.getChannel().force(true);

        this.storeStatsService.start();

        this.shutdown = false;
    }

    @Override
    public void shutdown() {

        if (!this.shutdown) {
            this.shutdown = true;

            try {

                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            this.storeStatsService.shutdown();

            if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
                this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
                shutDownNormal = true;
            } else {
                log.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
            }
        }

        if (lockFile != null && lock != null) {
            try {
                lock.release();
                lockFile.close();
            } catch (IOException e) {
            }
        }
    }

    @Override
    public void destroy() {
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }


    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is slave mode, so putMessage is forbidden ");
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        }

        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("message store is not writeable, so putMessage is forbidden " + this.runningFlags.getFlagBits());
            }

            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, null);
        } else {
            this.printTimes.set(0);
        }

        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
            return new PutMessageResult(PutMessageStatus.PROPERTIES_SIZE_EXCEEDED, null);
        }

        if (this.isOSPageCacheBusy()) {
            return new PutMessageResult(PutMessageStatus.OS_PAGECACHE_BUSY, null);
        }

        long beginTime = this.getSystemClock().now();

        PutMessageResult result = this.cacheData.putMessage(msg);

        long eclipseTime = this.getSystemClock().now() - beginTime;
        if (eclipseTime > 500) {
            log.warn("putMessage not in lock eclipse time(ms)={}, bodyLength={}", eclipseTime, msg.getBody().length);
        }
        this.storeStatsService.setPutMessageEntireTimeMax(eclipseTime);

        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }


    @Override
    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        return null;
    }

    @Override
    public GetMessageResult getMessage(String group, String topic, int queueId, long offset, int maxMsgNums, MessageFilter messageFilter) {

        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        GetMessageResult getMessageResult = new GetMessageResult();

        List<SelectMappedBufferResult> results = cacheData.getMessagesFromOffset(topic, offset, maxMsgNums);

        if (results == null || results.size() == 0) {
            getMessageResult.setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
            return getMessageResult;
        }

        getMessageResult.setStatus(GetMessageStatus.FOUND);
        for (SelectMappedBufferResult result : results) {
            getMessageResult.addMessage(result);
        }
        getMessageResult.setBufferTotalSize(results.size());

        return getMessageResult;

    }


    @Override
    public long getMaxOffsetInQueue(String topic, int queueId) {
        return 0;
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) {
        return 0;
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        return 0;
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        return 0;
    }

    @Override
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        return null;
    }


    public MessageStoreConfig getMessageStoreConfig() {

        return messageStoreConfig;

    }

    public SystemClock getSystemClock() {
        return systemClock;
    }

    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return null;
    }

    @Override
    public String getRunningDataInfo() {
        return null;
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        return null;
    }

    @Override
    public long getMaxPhyOffset() {
        return 0;
    }

    @Override
    public long getMinPhyOffset() {
        return 0;
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        return 0;
    }

    @Override
    public long getEarliestMessageTime() {
        return 0;
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        return 0;
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        return 0;
    }

    @Override
    public SelectMappedBufferResult getCommitLogData(long offset) {
        return null;
    }

    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        return false;
    }

    @Override
    public void executeDeleteFilesManually() {

    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        return null;
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {

    }

    @Override
    public long slaveFallBehindMuch() {
        return 0;
    }

    @Override
    public long now() {
        return 0;
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        return 0;
    }

    @Override
    public void cleanExpiredConsumerQueue() {

    }

    @Override
    public boolean checkInDiskByConsumeOffset(String topic, int queueId, long consumeOffset) {
        return false;
    }

    @Override
    public long dispatchBehindBytes() {
        return 0;
    }

    @Override
    public long flush() {
        return 0;
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return false;
    }

    @Override
    public long getConfirmOffset() {
        return 0;
    }

    @Override
    public void setConfirmOffset(long phyOffset) {

    }

    @Override
    public boolean isOSPageCacheBusy() {
        return false;
    }

    @Override
    public long lockTimeMills() {
        return 0;
    }

    @Override
    public boolean isTransientStorePoolDeficient() {
        return false;
    }

    @Override
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return null;
    }

    @Override
    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        return null;
    }


}
