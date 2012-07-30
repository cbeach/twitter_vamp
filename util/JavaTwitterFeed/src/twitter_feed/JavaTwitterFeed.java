package twitter_feed;
import net.sf.json.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.PipelineBlock;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.StringTokenizer;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import redis.clients.jedis.JedisPoolConfig;

/**
 *
 * @author mcsmash
 */
class tester implements Subscriber {
    public void receive(String message){
        if(message != null)
            System.out.println(message);
    }
    
}


public class JavaTwitterFeed {
    
    private JedisPool pool;
    private Jedis jedis;
    private String key;
    
    public static void main(String[] args) {
        System.out.println("starting feed");
        JavaTwitterFeed tf = new JavaTwitterFeed("localhost", "lang");
        try {
            tf.start_feed(new tester());
        } catch (Exception ex) {
            Logger.getLogger(JavaTwitterFeed.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public JavaTwitterFeed(String host, String redis_key) {
        this.pool = new JedisPool(new JedisPoolConfig(), "localhost");
        jedis = this.pool.getResource();
        this.key = redis_key;
    }
    public void start_feed(Subscriber obj) throws Exception {
        
        String message = null;
        while(true) {
            try {
                message = jedis.lpop(this.key);
            }
            catch(redis.clients.jedis.exceptions.JedisConnectionException e)
            {
                this.pool = new JedisPool(new JedisPoolConfig(), "localhost");
                jedis = this.pool.getResource();
            }
            catch(java.lang.ClassCastException e) {}
            if(message != null)
                obj.receive(message);
        }
    }    
    
    public void send(JSONObject message) throws InterruptedException {
        long count = 0;
        try {
            try {
                count = this.jedis.llen("lang_detected");
            }   
            catch (java.lang.NullPointerException e){
                jedis = this.pool.getResource();
            }
            if(count > 50000){
                while(count > 10000){
                    Thread.sleep(10000);
                    try {
                        count = this.jedis.llen("lang_detected");
                    }   
                    catch (java.lang.NullPointerException e){
                        jedis = this.pool.getResource();
                    }
                }
            }
            try {
                this.jedis.rpush("lang_detected", message.toString());
            }   
            catch (java.lang.NullPointerException e){
                jedis = this.pool.getResource();
            }
            
        }
        catch(java.lang.ClassCastException e) {}
    }

}


