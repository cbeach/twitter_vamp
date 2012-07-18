package detectlang;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import twitter_feed.Subscriber;
import twitter_feed.TwitterFeed;



/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author mcsmash
 */
public class DetectLang implements Subscriber {

    /**
     * @param args the command line arguments
     */
    Detector detect;
    TwitterFeed feed;
    public DetectLang(TwitterFeed tf) {
        this.feed = tf;
        try {
            DetectorFactory.loadProfile("/home/mcsmash/Documents/twitter_vamp/util/langdetect/profiles");
                      
        } catch (LangDetectException ex) {
            Logger.getLogger(DetectLang.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void main(String[] args) {
        System.out.println("starting feed");
        TwitterFeed tf = new TwitterFeed("localhost", "direct.raw");
        try {
            tf.start_feed(new DetectLang(tf));
        } catch (Exception ex) {
            Logger.getLogger(TwitterFeed.class.getName()).log(Level.SEVERE, null, ex);
        }
     
    }
    
    public void receive(String message) {
        try {
            this.detect = DetectorFactory.create();
        } catch (LangDetectException ex) {
            Logger.getLogger(DetectLang.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        JSONObject message_object = (JSONObject) JSONSerializer.toJSON(message);
        
        if(message_object.has("text") == true) {
        
            this.detect.append(message_object.getString("text"));
            String lang;
            
            try {
                lang = detect.detect();
                System.out.println(lang);
                JSONObject json = new JSONObject();
                json.put("id",message_object.getString("id"));
                json.put("text", message_object.getString("text"));
                json.put("lang",lang);
                this.feed.send(json);
            
            } catch (LangDetectException ex) {
                Logger.getLogger(DetectLang.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
    }
}
