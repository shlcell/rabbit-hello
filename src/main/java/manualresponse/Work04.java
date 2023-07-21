package manualresponse;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import utils.RabbitMqUtils;
import utils.SleepUtils;

import java.nio.charset.StandardCharsets;

public class Work04 {
    private static final String ACK_QUEUE_NAME = "ack_queue";

    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        System.out.println("C2 等待接收消息处理时间较长");

        // 设置不公平分发
        // int prefetchCount = 1;
        // 设置预取值
        int prefetchCount = 5;
        channel.basicQos(prefetchCount);

        // 消息者消费的时候如何处理消息
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            SleepUtils.sleep(30);
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("接收到消息:" + message);
            /*
             * 1.消息标记 tag
             * 2.是否批量应答未应答消息
             */
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        // 采用手动应答 autoAck:false
        channel.basicConsume(ACK_QUEUE_NAME, false, deliverCallback, (consumerTag) -> System.out.println(consumerTag + "消费者取消消费接口回调逻辑"));
    }

}
