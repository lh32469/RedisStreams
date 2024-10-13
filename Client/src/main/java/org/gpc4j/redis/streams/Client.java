package org.gpc4j.redis.streams;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 *
 * @author Lyle T Harris
 */
public class Client {

  static final String stream1 = "stream1";
  static final String group1 = "group1";

  private static final org.slf4j.Logger LOG
      = LoggerFactory.getLogger(Client.class);

  public static void main(String[] args) throws InterruptedException {

    final String consumer = UUID.randomUUID().toString();

    Random rand = new Random();

    // Exit after this many null replys for demo purposes.
    int maxAttempts = 5;

    JedisPool pool = new JedisPool("192.168.0.10", 7777);

    // Define Stream to retrieve from.
    Map.Entry<String, StreamEntryID> stream
        = Map.entry(stream1, StreamEntryID.UNRECEIVED_ENTRY);

    while (maxAttempts > 0) {
      LOG.info("------------------");

      try (Jedis jedis = pool.getResource()) {

        List<Map.Entry<String, List<StreamEntry>>> response
            = jedis.xreadGroup(group1, consumer, 1, 0, false, stream);

        // Blocking not working so well above with 'block' set to 0 
        // so need to check on response and then sleep..
        if (response != null) {
          for (Map.Entry<String, List<StreamEntry>> e1 : response) {
            //LOG.info("E1: " + e1);
            List<StreamEntry> streamEntry = e1.getValue();
            for (StreamEntry se : streamEntry) {
              LOG.debug("StreamEntry: " + se.getID());
              LOG.info("Recieved: " + se.getFields());

              jedis.xack(stream1, group1, se.getID());
              jedis.xtrim(stream1, 1000, true);
            }
          }
        } else {
          maxAttempts--;
        }

      } catch (JedisConnectionException ex) {
        LOG.warn(ex.getLocalizedMessage());
      }

      Thread.sleep(rand.nextInt(8000));
    }

  }

}
