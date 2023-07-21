package manualresponse;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import utils.RabbitMqUtils;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * 消息在手动应答时不丢失，重新放回队列中
 */
public class Task02 {
    private static final String TASK_QUEUE_NAME = "ack_queue";

    public static void main(String[] argv) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();

        // 消息队列持久化 durable:true
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        Scanner sc = new Scanner(System.in);
        System.out.println("请输入信息");
        while (sc.hasNext()) {
            String message = sc.nextLine();
            // 队列中的消息实现持久化:MessageProperties.PERSISTENT_TEXT_PLAIN
            channel.basicPublish(""
                    , TASK_QUEUE_NAME
                    , MessageProperties.PERSISTENT_TEXT_PLAIN
                    , message.getBytes(StandardCharsets.UTF_8));
            System.out.println("生产者发出消息" + message);
        }
    }
}
