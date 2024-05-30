package parser;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class Parser extends Thread {
    private static final String ARTICLES_QUEUE_NAME = "articles_queue";
    private static final String URL_QUEUE_NAME = "url_queue";
    private static final String RMQ_HOST_NAME = "localhost";
    private static final int RMQ_PORT = 5673;
    private static final String RMQ_USERNAME = "rmq_dev";
    private static final String RMQ_PASSWORD = "password";
    private static final String SERVER_URL = "http://localhost:9200";
    private static final String API_KEY = "";
    private static final String ARTICLES_INDEX = "articles";
    private static final Logger logger = LoggerFactory.getLogger(Parser.class);

    public void runLinkCatcher(int depth, String inputBaseUrl) throws IOException, TimeoutException, NoSuchAlgorithmException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RMQ_HOST_NAME);
        factory.setPort(RMQ_PORT);
        factory.setUsername(RMQ_USERNAME);
        factory.setPassword(RMQ_PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(ARTICLES_QUEUE_NAME, true, false, false, null);
        channel.basicQos(1);

        LinkCatcher prod = new LinkCatcher(depth, inputBaseUrl, SERVER_URL, API_KEY, ARTICLES_INDEX, URL_QUEUE_NAME, channel);
        prod.Start();

        channel.close();
        connection.close();
    }

    public void RunElkProducer(Map<String, Document> docVec) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RMQ_HOST_NAME);
        factory.setPort(RMQ_PORT);
        factory.setUsername(RMQ_USERNAME);
        factory.setPassword(RMQ_PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(ARTICLES_QUEUE_NAME, true, false, false, null);
        channel.basicQos(1);


        ElcProducer prod = new ElcProducer(channel, ARTICLES_QUEUE_NAME);

        prod.ParsePublishNews(docVec);

        channel.close();
        connection.close();
    }

    public void RunHtmlParserAndElcProducer() throws IOException, TimeoutException, InterruptedException {
        Map<String, Document> docVec = Collections.synchronizedMap(new ConcurrentHashMap<>());

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RMQ_HOST_NAME);
        factory.setPort(RMQ_PORT);
        factory.setUsername(RMQ_USERNAME);
        factory.setPassword(RMQ_PASSWORD);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(ARTICLES_QUEUE_NAME, true, false, false, null);
        channel.basicQos(1);

        HtmlParser cons = new HtmlParser(docVec);

        channel.basicConsume(URL_QUEUE_NAME, false, "javaConsumerTag", new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body)
                    throws IOException {
                long deliveryTag = envelope.getDeliveryTag();
                String message = new String(body, StandardCharsets.UTF_8);
                cons.parsePage(message);
                channel.basicAck(deliveryTag, false);
            }
        });

        int responceWaitCount = 0;

        final int retryCount = 5;

        while (responceWaitCount<retryCount) {
            AMQP.Queue.DeclareOk response = channel.queueDeclarePassive(URL_QUEUE_NAME);
            if (response.getMessageCount() != 0) {
                responceWaitCount = 0;
                Thread.sleep(500);
            } else {
                Thread.sleep(5000);
                responceWaitCount++;
                logger.debug("Waiting for messages in "+ URL_QUEUE_NAME +", " + (retryCount-responceWaitCount) * 5 + " seconds until shutdown" + Thread.currentThread());
            }
        }

        channel.basicCancel("javaConsumerTag");
        channel.close();
        connection.close();

        RunElkProducer(docVec);
    }

    public void RunElcConsumer() throws IOException, TimeoutException, InterruptedException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RMQ_HOST_NAME);
        factory.setPort(RMQ_PORT);
        factory.setUsername(RMQ_USERNAME);
        factory.setPassword(RMQ_PASSWORD);
        factory.setAutomaticRecoveryEnabled(true);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.basicQos(1);

        ElcConsumer cons = new ElcConsumer(SERVER_URL, API_KEY, ARTICLES_INDEX);

        channel.basicConsume(ARTICLES_QUEUE_NAME, false, "javaElcConsumerTag", new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body)
                    throws IOException {
                long deliveryTag = envelope.getDeliveryTag();
                String message = new String(body, StandardCharsets.UTF_8);
                logger.info(" Received '" + message + "'  " + Thread.currentThread());
                cons.consume(message);
                channel.basicAck(deliveryTag, false);
            }
        });

        int responceWaitCount = 0;

        final int retryCount = 100;

        while (responceWaitCount<retryCount) {
            AMQP.Queue.DeclareOk response = channel.queueDeclarePassive(ARTICLES_QUEUE_NAME);
            if (response.getMessageCount() != 0) {
                responceWaitCount = 0;
                Thread.sleep(5000);
            } else {
                Thread.sleep(5000);
                responceWaitCount++;
                logger.debug("Waiting for messages in "+ ARTICLES_QUEUE_NAME +", " + (retryCount-responceWaitCount) * 5 + " seconds until shutdown" + Thread.currentThread());
            }
        }

        try {
            channel.basicCancel("javaElcConsumerTag");
            channel.close();
            connection.close();
        } catch (IOException | TimeoutException e) {
            logger.error(String.valueOf(e));
        }
    }

}