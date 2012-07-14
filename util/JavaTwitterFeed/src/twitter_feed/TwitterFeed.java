package twitter_feed;
import net.sf.json.*;

import java.util.StringTokenizer;

import java.io.IOException;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author mcsmash
 */
class tester implements Subscriber {
    public void receive(String message){
        System.out.println(message);
    }
    
}


public class TwitterFeed {
    
    ConnectionFactory factory;
    Connection connection;
    Channel channel;
    QueueingConsumer consumer;
    
    public static void main(String[] args) {
        System.out.println("starting feed");
        TwitterFeed tf = new TwitterFeed("localhost", "direct.raw");
        try {
            tf.start_feed(new tester());
        } catch (Exception ex) {
            Logger.getLogger(TwitterFeed.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public TwitterFeed(String host, String exchange_name) {
        try {
            this.factory = new ConnectionFactory();
            this.factory.setHost(host);
            this.connection = factory.newConnection();
            this.channel = connection.createChannel();

            this.channel.exchangeDeclare(exchange_name, "fanout");
            this.channel.exchangeDeclare("direct.lang", "direct");
            String queueName = this.channel.queueDeclare().getQueue();
            this.channel.queueBind(queueName, exchange_name, "raw");

            this.consumer = new QueueingConsumer(channel);
            this.channel.basicConsume(queueName, true, consumer);
        } catch (IOException ex) {
            System.out.println("exception in constructor");
            System.out.println(ex.getMessage());
        }
        
    }
    public void start_feed(Subscriber obj) throws Exception {

        while(true) {
            QueueingConsumer.Delivery delivery = this.consumer.nextDelivery();
            String message = new String(delivery.getBody());
            obj.receive(message);
        }
    }    
    
    public void send(JSONObject message) {
        try {
            this.channel.basicPublish("direct.lang","lang",null,message.toString().getBytes());
        } catch (IOException ex) {
            Logger.getLogger(TwitterFeed.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}


