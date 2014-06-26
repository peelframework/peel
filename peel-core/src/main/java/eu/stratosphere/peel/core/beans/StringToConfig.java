package eu.stratosphere.peel.core.beans;

import org.springframework.core.convert.converter.Converter;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class StringToConfig implements Converter<String, Config> {

    @Override
    public Config convert(String s) {
        return ConfigFactory.parseString(s);
    }
}
