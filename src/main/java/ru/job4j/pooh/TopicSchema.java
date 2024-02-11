package ru.job4j.pooh;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class TopicSchema implements Schema {
    private final ConcurrentHashMap<String, ConcurrentHashMap<Receiver, BlockingQueue<String>>> data = new ConcurrentHashMap<>();
    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        data.putIfAbsent(receiver.name(), new ConcurrentHashMap<>());
        data.get(receiver.name()).putIfAbsent(receiver, new LinkedBlockingQueue<>());
        condition.on();
    }

    @Override
    public void publish(Message message) {
        data.putIfAbsent(message.name(), new ConcurrentHashMap<>());
        var mapReceivers = data.get(message.name());
        for (var queueEntry : mapReceivers.entrySet()) {
            queueEntry.getValue().add(message.text());
        }
        condition.on();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            for (var receiversOnTopic : data.values()) {
                for (var receiverWithQueue : receiversOnTopic.entrySet()) {
                    while (true) {
                        var data = receiverWithQueue.getValue().poll();
                        if (data != null) {
                            receiverWithQueue.getKey().receive(data);
                        }
                        if (data == null) {
                            break;
                        }
                    }
                }
            }
            condition.off();
            try {
                condition.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
