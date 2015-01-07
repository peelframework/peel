package eu.stratosphere.peel.analyser.util;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.reflections.Reflections;
import scala.tools.nsc.backend.icode.Primitives;

import java.util.Set;

/**
 * Created by Fabsi on 21.10.2014.
 */
public class HibernateUtil {
    private static final ORMUtil ormUtil = new ORMUtil();
    private static final SessionFactory sessionFACTORY = buildSessionFactory();

    private static SessionFactory buildSessionFactory() {
        try {
            return new Configuration().configure().buildSessionFactory();
        } catch (Throwable e){
            throw new ExceptionInInitializerError(e);
        }
    }

    public static SessionFactory getSessionFACTORY(){
        return sessionFACTORY;
    }

    public static org.hibernate.Session getSession(){
        if(sessionFACTORY.getCurrentSession() == null){
            return sessionFACTORY.openSession();
        } else {
            return sessionFACTORY.getCurrentSession();
        }
    }

    public static ORMUtil getORM(){
        return ormUtil;
    }

    public static void deleteAll() {
        String deleteString = "delete * from :modelname";
        Reflections reflections = new Reflections("eu.stratosphere.peel.analyser.model");
        Set<Class<? extends Object>> models = reflections.getSubTypesOf(Object.class);
        for(Class<? extends Object> model: models){
            ormUtil.executeQuery(model, deleteString, new QueryParameter("modelname", model.getName()));
        }
    }

}
