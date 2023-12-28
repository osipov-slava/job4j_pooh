package ru.job4j.pooh;

import java.util.concurrent.*;

public class QueueSchema implements Schema {
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Receiver>> receivers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BlockingQueue<String>> data = new ConcurrentHashMap<>();
    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        receivers.putIfAbsent(receiver.name(), new CopyOnWriteArrayList<>());//добавить новою ПОДПИСКУ
        receivers.get(receiver.name()).add(receiver);//Подписать на подписку получателя
        condition.on();
    }

    @Override
    public void publish(Message message) {
        data.putIfAbsent(message.name(), new LinkedBlockingQueue<>()); //добавить ПОДПИСКУ
        data.get(message.name()).add(message.text()); //добавить текст сообщения в очередь ПОДПИСКИ
        condition.on();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            for (var queueKey : receivers.keySet()) {//сет ключей/имен ПОДПИСОК
                var queue = data.getOrDefault(queueKey, new LinkedBlockingQueue<>());//очередь сообщений по ПОДПИСКЕ
                var receiversByQueue = receivers.get(queueKey); //текущий список получателей на ПОДПИСКУ
                var it = receiversByQueue.iterator(); // итератор списка получателей
                while (it.hasNext()) { //если есть еще получатель
                    var data = queue.poll(); //взять и удалить сообщение из очереди
                    if (data != null) {
                        it.next().receive(data); //если сообщение есть передать сообщение получателю
                    }
                    if (data == null) { //если сообщений нет или больше нет, то работаем со след ПОДПИСКОЙ
                        break;
                    }
                    if (!it.hasNext()) {//запустить список получателей заново
                        it = receiversByQueue.iterator();
                    }
                }
            }
            condition.off();//все очереди сообщений отработаны
            try {
                condition.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
