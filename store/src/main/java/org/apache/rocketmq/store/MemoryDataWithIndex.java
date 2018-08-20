package org.apache.rocketmq.store;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: rocketmq-all
 * @description:
 * @author: xdCao
 * @create: 2018-08-20 20:37
 **/
public class MemoryDataWithIndex {

    private AtomicInteger nextIndex = new AtomicInteger(0);

    private List<MessageExtBrokerInner> dataList = new ArrayList<>(32);

    public MemoryDataWithIndex() {
    }

    public MessageExtBrokerInner getMessage () {
        return dataList.get(nextIndex.getAndIncrement());
    }

    public MessageExtBrokerInner getMessage (Integer offset) {
        if (offset >= dataList.size()) {
            return null;
        }
        return dataList.get(offset);
    }


    public synchronized void putMessage(MessageExtBrokerInner msg) {
        dataList.add(msg);
        nextIndex.incrementAndGet();
    }


    public AtomicInteger getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(AtomicInteger nextIndex) {
        this.nextIndex = nextIndex;
    }

    public List<MessageExtBrokerInner> getDataList() {
        return dataList;
    }

    public void setDataList(List<MessageExtBrokerInner> dataList) {
        this.dataList = dataList;
    }
}
