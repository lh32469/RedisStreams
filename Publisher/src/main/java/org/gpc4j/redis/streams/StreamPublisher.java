package org.gpc4j.redis.streams;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimerTask;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;

/**
 *
 * @author Lyle T Harris
 */
public class StreamPublisher extends TimerTask {

  private final JedisPool pool;
  private final String stream;
  private final List<String> firstNames = new LinkedList<>();
  private final List<String> lastNames = new LinkedList<>();
  private final Random rand = new Random();

  private static final org.slf4j.Logger LOG
      = LoggerFactory.getLogger(StreamPublisher.class);

  public StreamPublisher(JedisPool pool, String streamName) {
    this.pool = pool;
    this.stream = streamName;

    List<String> temp = Arrays.asList((names1 + names2).split("\n"));
    for (String string : temp) {
      String[] name = string.trim().split(" ");
      firstNames.add(name[0].trim());
      lastNames.add(name[1].trim());
    }

    LOG.debug("First Names: {} ", firstNames);
    LOG.debug("Last Names: {}", lastNames);
  }

  @Override
  public void run() {

    Map<String, String> name = getRandomName();

    try (Jedis jedis = pool.getResource()) {
      StreamEntryID id = jedis.xadd(stream, StreamEntryID.NEW_ENTRY, name);
      jedis.save();
      LOG.debug("Published: " + id);
      LOG.info("Published: " + name);
    }

  }

  Map<String, String> getRandomName() {
    Map<String, String> name = new HashMap<>();
    name.put("firstName", firstNames.get(rand.nextInt(firstNames.size() - 1)));
    name.put("lastName", lastNames.get(rand.nextInt(lastNames.size() - 1)));
    return name;
  }

  //<editor-fold defaultstate="collapsed" desc="Random Names">
  private final String names1 = "Teofila Lamothe  \n"
      + "Lean Sroka  \n"
      + "Dani Mouzon  \n"
      + "Lucilla Pollan  \n"
      + "India Ronan  \n"
      + "Melba Erby  \n"
      + "Lucie Traina  \n"
      + "Minda Duet  \n"
      + "Tobie Robey  \n"
      + "Keira Lyles  \n"
      + "Johnetta Bouyer  \n"
      + "Maryellen Brookman  \n"
      + "Stefania Santi  \n"
      + "Adelle Blanchard  \n"
      + "Karole Mccullum  \n"
      + "Gilda Ganser  \n"
      + "Kelsey Sitton  \n"
      + "Sherell Aikin  \n"
      + "Jennell Fray  \n"
      + "Cathey Tousignant  \n"
      + "Verna Vandehey  \n"
      + "Maggie Mcgavock  \n"
      + "Elissa Luczynski  \n"
      + "Consuela Howery  \n"
      + "Lucina Croyle  \n"
      + "Warren Shepherd  \n"
      + "Shea Sentell  \n"
      + "Lili Flippen  \n"
      + "Ervin Kohls  \n"
      + "Eleanor Spang  \n"
      + "Alexis Philbeck  \n"
      + "Eugenio Guido  \n"
      + "Ismael Machuca  \n"
      + "Yuko Quarterman  \n"
      + "Neta Keese  \n"
      + "Stuart Abron  \n"
      + "Kaitlin Hosey  \n"
      + "Israel Candela  \n"
      + "Lorenzo Sowers  \n"
      + "Ike Bath  \n"
      + "Jasmine Hooks  \n"
      + "Abby Ratti  \n"
      + "Hilario Worrell  \n"
      + "Eugena Patridge  \n"
      + "Emilie Ver  \n"
      + "Minna Mcdade  \n"
      + "Samantha Houpt  \n"
      + "Veda Catlin  \n"
      + "Logan Chappelle  \n"
      + "Antonetta Lipsky  \n";

  private final String names2 = "Armando Baucom  \n"
      + "Silas Desper  \n"
      + "Dakota Copland  \n"
      + "Jessica Saavedra  \n"
      + "Valorie Taveras  \n"
      + "Tula Schwantes  \n"
      + "Ginny Granger  \n"
      + "Bell Littler  \n"
      + "Catheryn Cohee  \n"
      + "Iris Magallanes  \n"
      + "Hugh Gilbertson  \n"
      + "Chu Sama  \n"
      + "Vaughn Longoria  \n"
      + "Kary Fenn  \n"
      + "Osvaldo Ellerman  \n"
      + "Dannette Poehler  \n"
      + "Sybil Sidener  \n"
      + "Kirby Ethridge  \n"
      + "Gabriele Dallas  \n"
      + "Mari Bothwell  \n"
      + "Giovanna Dessert  \n"
      + "Elli Sprau  \n"
      + "Hellen Dostie  \n"
      + "Gertie Pages  \n"
      + "Joye Savidge  \n"
      + "Salome Ang  \n"
      + "Talia Kjos  \n"
      + "Jenee Yurick  \n"
      + "Ronni Heintzelman  \n"
      + "Larraine Manes  \n"
      + "Miesha Mcgahey  \n"
      + "Gerda Mujica  \n"
      + "Cher Acton  \n"
      + "Eula Huckins  \n"
      + "Sebastian Ranger  \n"
      + "Dorinda Cisco  \n"
      + "Cory Tindal  \n"
      + "Katherina Macedo  \n"
      + "Mirtha Straka  \n"
      + "Kimberly Hooks  \n"
      + "Mora Heidt  \n"
      + "Ivelisse Gasaway  \n"
      + "Tanya Farabee  \n"
      + "Judy Koehl  \n"
      + "Anitra Ireland  \n"
      + "Annie Bunge  \n"
      + "Mayme Troutman  \n"
      + "Weldon Briganti  \n"
      + "Dewey Eiland  \n"
      + "Lashunda Mattis  \n";
//</editor-fold>
}
