package detectlang;

import net.sf.json.*;
import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Iterator;
import twitter_feed.*;



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
    int tweets_recieved;
    
    Detector detect;
    JavaTwitterFeed feed;
    
    public DetectLang(JavaTwitterFeed tf) {
        this.feed = tf;
        this.tweets_recieved = 0;
        try {
            DetectorFactory.loadProfile("/home/mcsmash/Documents/projects/twitter_vamp/util/langdetect/profiles");
                      
        } catch (LangDetectException ex) {
            Logger.getLogger(DetectLang.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void main(String[] args) {
        System.out.println("starting detector feed");
        JavaTwitterFeed tf = new JavaTwitterFeed("localhost", "lang_in");
        try {
            tf.start_feed(new DetectLang(tf));
        } catch (Exception ex) {
            Logger.getLogger(JavaTwitterFeed.class.getName()).log(Level.SEVERE, null, ex);
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
//                System.out.println(lang);
                JSONObject json = new JSONObject();
//                json.put("id",message_object.getString("id"));
//                json.put("text", message_object.getString("text"));
//                json.put("lang",lang);
//                this.feed.send(json);
                message_object.put("lang",lang);
                this.feed.send(message_object);
            } catch (LangDetectException ex) {
                Logger.getLogger(DetectLang.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        this.tweets_recieved += 1;
        if(this.tweets_recieved == 25000) {
            this.tweets_recieved = 0;
            System.out.println("creating new tf");
            this.feed = new JavaTwitterFeed("localhost", "lang_in");
            try {
                this.feed.start_feed(this);
            } catch (Exception ex) {
               Logger.getLogger(JavaTwitterFeed.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
