package eu.stratosphere.peel.analyser.util;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.reflections.Reflections;
import scala.tools.nsc.backend.icode.Primitives;

import javax.persistence.Entity;
import java.util.Properties;
import java.util.Set;

/**
 * Created by Fabsi on 21.10.2014.
 */
public class HibernateUtil {
    public static ORM ormUtil = null;

    public static ORM getORM(){
        if(ormUtil == null)
            initORM();
        return ormUtil;
    }

    public static void initORM() {
        ormUtil = new ORMUtil();
    }
    public static void deleteAll() {
        ORMUtil.createSessionFactory();
    }

}
