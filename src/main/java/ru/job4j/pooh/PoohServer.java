package ru.job4j.pooh;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PoohServer {
    private final QueueSchema queueSchema = new QueueSchema();
    private final TopicSchema topicSchema = new TopicSchema();

    private void runSchemas() {
        ExecutorService pool = Executors.newCachedThreadPool(); //пул способов обработки
        pool.execute(queueSchema);
        pool.execute(topicSchema);
    }

    private void runServer() {
        ExecutorService pool = Executors.newCachedThreadPool();
        try (ServerSocket server = new ServerSocket(9000)) { //один сервер
            System.out.println("Pooh is ready ...");
            while (!server.isClosed()) {
                Socket socket = server.accept(); //сокет для каждого приложения
                pool.execute(() -> { //пул сокетов
                    try (OutputStream out = socket.getOutputStream();
                         var input = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                        while (true) {
                            var details = input.readLine().split(";"); //читаем сообщение и десериализуем его
                            if (details.length != 3) {
                                continue;
                            }
                            var action = details[0]; //регистрация, очередь/топик
                            var name = details[1]; //если регистрация, то быть клиентом очереди или топика
                                                //если сообщение, то ПОДПИСКА
                            var text = details[2]; //если регистрация, то имя клиента
                                                    //если сообщение, то текст
                            if (action.equals("intro")) {
                                if (name.equals("queue")) {
                                    queueSchema.addReceiver(
                                            new SocketReceiver(text, new PrintWriter(out)) //добавляем получателя (с именем и каналом передачи)
                                    );
                                }
                                if (name.equals("topic")) {
                                    topicSchema.addReceiver(
                                            new SocketReceiver(text, new PrintWriter(out))
                                    );
                                }
                            }
                            if (action.equals("queue")) {
                                queueSchema.publish(new Message(name, text));//новое сообщение (ПОДПИСКА,текст)
                            }
                            if (action.equals("topic")) {
                                topicSchema.publish(new Message(name, text));
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        var pooh = new PoohServer();
        pooh.runSchemas();
        pooh.runServer();
    }
}