package eu.stratosphere.peel.core.beans;

import eu.stratosphere.peel.core.beans.system.Lifespan;
import org.springframework.core.convert.converter.Converter;
import scala.Enumeration.Value;

public class StringToLifespan implements Converter<String, Value> {

    @Override
    public Value convert(String s) {
        if (s.equals("PROVIDED")) {
            return Lifespan.PROVIDED();
        } else if (s.equals("SUITE")) {
            return Lifespan.SUITE();
        } else if (s.equals("EXPERIMENT")) {
            return Lifespan.EXPERIMENT();
        } else {
            throw new IllegalArgumentException(s + " can not be converted to Scala Lifecycle Value!");
        }
    }
}
