package eu.stratosphere.peel.analyser.util;

import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Example;
import org.hibernate.criterion.Restrictions;
import org.hibernate.service.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Enable various ORM Operations based on hibernate
 */
public class ORMUtil {

  private static final SessionFactory SESSION_FACTORY;
  private static final Logger LOGGER = LoggerFactory.getLogger(ORMUtil.class);

  /**
   * creates a new ORMUtil.
   */
  public ORMUtil() {

  }

  //this is to initialize the HibernateConfiguration
  static {
    try {
      //the Hibernate configuration
      Configuration configuration = new Configuration();

      //Hibernate will search for the hibernate.cfg.xml configuration and Hibernate will load the default properties there into the configuration.
      configuration.configure("hibernate.cfg.xml");


      //register the Hibernate configuration
      StandardServiceRegistryBuilder registry = new StandardServiceRegistryBuilder();
      registry.applySettings(configuration.getProperties());
      ServiceRegistry serviceRegistry = registry.build();

      //get the SESSION_FACTORY
      SESSION_FACTORY = configuration.buildSessionFactory(serviceRegistry);
      LOGGER.info("Hibernate connection established successfully.");
    } catch (Exception ex) {
      LOGGER.error("Initialisation of Hibernate configuration failed.", ex);
      throw new ExceptionInInitializerError(ex);
    }
  }

  /**
   * gets the current session, or, if the currentSession is null, creates a new one
   *
   * @return current Session
   */
  private Session getSession() {
    Session session = SESSION_FACTORY.getCurrentSession();
    return session != null ? session : SESSION_FACTORY.openSession();
  }

  /**
   * begin a hibernate transaction
   */
  public void beginTransaction() {
    getSession().beginTransaction();
  }

  /**
   * commit a hibernate transaction
   */
  public void commitTransaction() {
    getSession().getTransaction().commit();
  }

  /**
   * close a hibernate session
   */
  public void closeSession() {
    getSession().close();
  }

  /**
   * deletes a object in the database
   *
   * @param obj The database Object to be deleted
   */
  public void delete(Object obj) {
    getSession().delete(obj);
  }

  /**
   * Updates the object in the database
   *
   * @param obj The database Object to be updated
   */
  public void update(Object obj) {
    getSession().update(obj);
  }

  /**
   * Saves Object obj in the database.
   *
   * @param obj The database Object to be saved
   */
  public void save(Object obj) {
    getSession().save(obj);
  }

  /**
   * Gets the results of a HQL query as a list
   *
   * @param query A HQL query
   * @param clazz The type of the resulting objects
   * @param params The query parameters
   * @return result as a List
   */
  @SuppressWarnings("unchecked")
  public <T> List<T> executeQuery(Class<T> clazz, String query, QueryParameter ... params) {
    List<T> result;
    Query databaseQuery = getSession().createQuery(query);
    for (QueryParameter param: params){
      databaseQuery.setParameter(param.getKey(), param.getValue());
    }
    result = (List<T>) databaseQuery.list();
    return result;
  }

  /**
   * Executes a Query by a example object. The query will be created to return all objects
   * that have the same fields as the example object.
   * @param clazz The class of the example
   * @param example The example object
   * @param <T> The type
   * @return The resulting list
   */
  public <T> List<T> executeQueryByExample(Class<T> clazz, T example){
    Criteria criteria = getSession().createCriteria(clazz);
    Example exampleHibernate = Example.create(example);
    return criteria.add(exampleHibernate).list();
  }

  /**
   * This method gets all Objects of classT that are stored in the database
   *
   * @param classT The class of the objects you want to have
   * @return a list of database entries
   */
  @SuppressWarnings("unchecked")
  public <T> List<T> getEntriesOfType(Class<T> classT) {
    return (List<T>) getSession().createCriteria(classT).list();
  }
}
