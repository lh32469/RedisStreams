package org.gpc4j.redis.streams;

import java.util.Timer;
import java.util.TimerTask;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.circuitbreaker.EnableCircuitBreaker;
import org.springframework.cloud.netflix.hystrix.dashboard.EnableHystrixDashboard;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 *
 * @author Lyle T Harris
 */
@SpringBootApplication
@EnableCircuitBreaker
@EnableHystrixDashboard
@RestController
public class ApplicationMain {

  private static final org.slf4j.Logger LOG
      = LoggerFactory.getLogger(ApplicationMain.class);

  @RequestMapping("/greet")
  public String showGreeting() {
    return this.toString();
  }

  public static void main(String[] args) {

    final String stream1 = "stream1";
    final String group1 = "group1";

    // docker run -v /tmp/redis:/data -d -p 7777:6379 --name redis-demo redis
    final JedisPool pool = new JedisPool("192.168.0.10", 7777);

    // Create group1
    try (Jedis jedis = pool.getResource()) {
      LOG.info("Group Created: "
          + jedis.xgroupCreate(stream1, group1, StreamEntryID.LAST_ENTRY, true));
    } catch (JedisDataException ex) {
      // Already created
      LOG.info(ex.toString());
    }

    // Start publishing to stream1
    TimerTask pub1 = new StreamPublisher(pool, stream1);
    Timer timer = new Timer();
    // Wait ten seconds and then run every two seconds after that
    timer.scheduleAtFixedRate(pub1, 10000, 2000);

    SpringApplication.run(ApplicationMain.class, args);
  }

}
