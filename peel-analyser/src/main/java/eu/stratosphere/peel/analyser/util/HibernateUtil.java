package eu.stratosphere.peel.analyser.util;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

/**
 * Created by Fabsi on 21.10.2014.
 */
public class HibernateUtil {
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

}
