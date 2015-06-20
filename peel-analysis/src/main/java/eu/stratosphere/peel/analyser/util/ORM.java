package eu.stratosphere.peel.analyser.util;

import org.hibernate.SessionFactory;

import java.util.List;

/**
 * Provides basic Access to the Database to create, read, update and delete POJO Objects
 */
public interface ORM {

  /**
   * Deletes a object in the database
   *
   * @param obj obj The database Object to be deleted
   */
  public void delete(Object obj);

  /**
   * Synchronizes a object with the database
   *
   * @param obj obj The database Object to be updated
   */
  public void update(Object obj);

  /**
   * Saves Object obj in the database. This method is Thread-safe.
   *
   * @param obj obj The database Object to be saved
   */
  public void save(Object obj);

  /**
   * Gets the results of a HQL query as a list
   *
   * @param query The HQL query
   * @param clazz The type of the resulting objects
   * @param params Query Parameter to add to the query to avoid hql injection
   * @param <T>   the type parameter
   * @return result as a List
   */
  public <T> List<T> executeQuery(Class<T> clazz, String query,
                  QueryParameter... params);

  /**
   * begin a hibernate transaction
   */
  public void beginTransaction();

  /**
   * commit a hibernate transaction
   */
  public void commitTransaction();

  /**
   * This method gets all Objects of classT that are stored in the database
   *
   * @param classT The class of the objects you want to have
   * @param <T>    the type parameter
   * @return a List of all Database Entries of this type
   */
  public <T> List<T> getEntriesOfType(Class<T> classT);

  public SessionFactory getSessionFactory();
}
