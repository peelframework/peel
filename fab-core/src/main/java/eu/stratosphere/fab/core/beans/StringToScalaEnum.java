package eu.stratosphere.fab.core.beans;

import eu.stratosphere.fab.core.beans.system.Lifespan;
import org.springframework.core.convert.converter.Converter;
import scala.Enumeration.Value;

/**
 * Created by felix on 09.06.14.
 */
public class StringToScalaEnum implements Converter<String, Value>{

    public Value convert(String s) {
        if(s.equals("SUITE")) {
            return Lifespan.SUITE();
        }
        else if(s.equals("EXPERIMENT_SEQUENCE")) {
            return Lifespan.EXPERIMENT_SEQUENCE();
        }
        else if(s.equals(("EXPERIMENT"))) {
            return Lifespan.EXPERIMENT();
        }
        else if(s.equals("EXPERIMENT_RUN")) {
            return Lifespan.EXPERIMENT_RUN();
        }
        else {
            throw new IllegalArgumentException(s + "can not be converted to Scala Lifecycle Value!");
        }
    }

}
