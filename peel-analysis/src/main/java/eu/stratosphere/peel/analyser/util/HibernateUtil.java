package eu.stratosphere.peel.analyser.util;


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
