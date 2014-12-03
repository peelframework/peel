package eu.stratosphere.peel.analyser.model;

class HibernateDatabaseTest {
/*
    @After
    public void cleanUp(){
        HibernateUtil.resetDatabase();
    }
    @Test
    public void createSystem() throws Exception{
        String systemName = "flink";

        List result = null;
        Session session = null;
        try {
            session = HibernateUtil.getSessionFACTORY().openSession();
            session.beginTransaction();

            System system = new System();
            system.setName(systemName);
            session.save(system);

            session.getTransaction().commit();
        } catch (Exception e){
            if(session != null) {
                session.getTransaction().rollback();
            }
            throw e;
        }

        result = session.createQuery("from System").list();        //Case sensitive!
        session.close();

        assertEquals(systemName, ((System)result.get(0)).getName());
    }


    @Test
    public void testExperimentSuite() throws Exception {
        String experimentSuiteName = "wc.wordcount.single-run";

        List result = null;
        Session session = null;
        try {
            session = HibernateUtil.getSessionFACTORY().openSession();
            session.beginTransaction();

            ExperimentSuite experimentSuite = new ExperimentSuite();
            experimentSuite.setName(experimentSuiteName);
            session.save(experimentSuite);

            session.getTransaction().commit();
        } catch (Exception e){
            if(session != null) {
                session.getTransaction().rollback();
            }
            throw e;
        }

        result = session.createQuery("from ExperimentSuite").list();        //Case sensitive!
        session.close();

        assertEquals(experimentSuiteName, ((ExperimentSuite)result.get(0)).getName());
    }

    @Test
    public void testExperimentSuiteJoinExperiment() throws Exception {
        String experimentSuiteName = "wc.wordcount.single-run";
        String experimentName = "wordcount";
        int experimentRuns = 5;

        List resultSuite = null;
        Iterator<Experiment> resultExperimentIterator = null;
        Session session = null;
        try {
            session = HibernateUtil.getSessionFACTORY().openSession();
            session.beginTransaction();

            ExperimentSuite experimentSuite = new ExperimentSuite();
            experimentSuite.setName(experimentSuiteName);
            session.save(experimentSuite);

            System system = new System();
            system.setName("flink");
            session.save(system);

            Experiment experiment = new Experiment();
            experiment.setExperimentSuite(experimentSuite);
            experiment.setName(experimentName);
            experiment.setRuns(experimentRuns);
            experiment.setSystem(system);
            system.getExperimentSet().add(experiment);
            session.save(experiment);

            experimentSuite.getExperimentSet().add(experiment);

            session.getTransaction().commit();
        } catch (Exception e){
            if(session != null) {
                session.getTransaction().rollback();
            }
            throw e;
        }

        resultSuite = session.createQuery("from ExperimentSuite").list();        //Case sensitive!
        resultExperimentIterator = ((ExperimentSuite)resultSuite.get(1)).getExperimentSet().iterator();
        Experiment experimentResult = resultExperimentIterator.next();
        session.close();

        assertEquals(experimentSuiteName, ((ExperimentSuite)resultSuite.get(1)).getName());
        assertEquals(experimentName, experimentResult.getName());
        assertEquals(experimentRuns, experimentResult.getRuns());
    }

    public void testSuiteJoinExpJoinRun() throws Exception{
        String experimentSuiteName = "wc.wordcount.single-run";
        String experimentName = "wordcount";
        int experimentRuns = 5;
        int experimentRunRun = 1;

        List resultSuite = null;
        Iterator<Experiment> resultExperimentIterator = null;
        Session session = null;
        try {
            session = HibernateUtil.getSessionFACTORY().openSession();
            session.beginTransaction();

            ExperimentSuite experimentSuite = new ExperimentSuite();
            experimentSuite.setName(experimentSuiteName);
            session.save(experimentSuite);

            System system = new System();
            system.setName("flink");
            session.save(system);

            Experiment experiment = new Experiment();
            experiment.setExperimentSuite(experimentSuite);
            experiment.setName(experimentName);
            experiment.setRuns(experimentRuns);
            experiment.setSystem(system);
            system.getExperimentSet().add(experiment);
            session.save(experiment);

            ExperimentRun experimentRun = new ExperimentRun();
            experimentRun.setExperiment(experiment);
            experimentRun.setRun(1);
            session.save(experimentRun);
            experiment.getExperimentRunSet().add(experimentRun);

            experimentSuite.getExperimentSet().add(experiment);
            session.close();

            session.getTransaction().commit();
        } catch (Exception e){
            if(session != null) {
                session.getTransaction().rollback();
            }
            throw e;
        }

        resultSuite = session.createQuery("from ExperimentSuite").list();        //Case sensitive!
        session.close();
        resultExperimentIterator = ((ExperimentSuite)resultSuite.get(0)).getExperimentSet().iterator();
        Experiment experimentResult = resultExperimentIterator.next();
        Iterator<ExperimentRun> experimentRunIterator = experimentResult.getExperimentRunSet().iterator();
        ExperimentRun experimentRunResult = experimentRunIterator.next();

        assertEquals(experimentSuiteName, ((ExperimentSuite)resultSuite.get(0)).getName());
        assertEquals(experimentName, experimentResult.getName());
        assertEquals(experimentRuns, experimentResult.getRuns());
        assertEquals(experimentRunRun, experimentRunResult.getRun());
    }

    @Test
    public void testEntireModel() throws Exception{
        String experimentSuiteName = "wc.wordcount.single-run";
        String experimentName = "wordcount";
        int experimentRuns = 5;
        int experimentRunRun = 1;
        Integer subtaskNumber = 1;
        Integer taskNumberOfSubtask = 1;

        List resultSuite = null;
        Iterator<Experiment> resultExperimentIterator = null;
        Session session = null;
        TaskInstance taskInstance = null;
        try {
            //create session
            session = HibernateUtil.getSessionFACTORY().openSession();
            session.beginTransaction();

            //create Experiment Suite
            ExperimentSuite experimentSuite = new ExperimentSuite();
            experimentSuite.setName(experimentSuiteName);
            session.save(experimentSuite);

            System system = new System();
            system.setName("flink");
            session.save(system);

            //create Experiment and connect it to ExperimentSuite
            Experiment experiment = new Experiment();
            experiment.setExperimentSuite(experimentSuite);
            experiment.setName(experimentName);
            experiment.setRuns(experimentRuns);
            experiment.setSystem(system);
            system.getExperimentSet().add(experiment);
            session.save(experiment);
            experimentSuite.getExperimentSet().add(experiment);

            //create ExperimentRun and add it to Experiment
            ExperimentRun experimentRun = new ExperimentRun();
            experimentRun.setExperiment(experiment);
            experimentRun.setRun(experimentRunRun);
            session.save(experimentRun);
            experiment.getExperimentRunSet().add(experimentRun);


            //create Task and add it to TaskType and ExperimentRun
            Task task = new Task();
            task.setTaskType("CHAIN");
            task.setExperimentRun(experimentRun);
            task.setNumberOfSubtasks(taskNumberOfSubtask);
            session.save(task);
            task.getTaskID();
            experimentRun.getTaskSet().add(task);

            //create TaskInstance and add it to the task
            taskInstance = new TaskInstance();
            taskInstance.setTask(task);
            taskInstance.setSubTaskNumber(subtaskNumber);
            session.save(taskInstance);
            task.getTaskInstances().add(taskInstance);

            //commit the transaction
            session.getTransaction().commit();
        } catch (Exception e){
            if(session != null) {
                session.getTransaction().rollback();
            }
            throw e;
        }

        //get the results from Database
        resultSuite = session.createQuery("from ExperimentSuite").list();        //Case sensitive!
        resultExperimentIterator = ((ExperimentSuite)resultSuite.get(2)).getExperimentSet().iterator();
        Experiment experimentResult = resultExperimentIterator.next();
        Iterator<ExperimentRun> experimentRunIterator = experimentResult.getExperimentRunSet().iterator();
        ExperimentRun experimentRunResult = experimentRunIterator.next();
        Iterator<Task> taskIterator = experimentRunResult.getTaskSet().iterator();
        Task taskResult = taskIterator.next();
        Iterator<TaskInstance> taskInstanceIterator = taskResult.getTaskInstances().iterator();
        TaskInstance taskInstanceResult = taskInstanceIterator.next();
        session.close();

        //check if saved database values are as expected
        assertEquals(experimentSuiteName, ((ExperimentSuite)resultSuite.get(2)).getName());
        assertEquals(experimentName, experimentResult.getName());
        assertEquals(experimentRuns, experimentResult.getRuns());
        assertEquals(experimentRunRun, experimentRunResult.getRun());
        assertEquals(taskInstanceResult.getSubTaskNumber(), subtaskNumber);
        assertEquals(taskResult.getNumberOfSubtasks(), taskNumberOfSubtask);
    }

    @Test
    public void testManyTasks() throws Exception {
        String experimentSuiteName = "wc.wordcount.single-run";
        String experimentName = "wordcount";
        int experimentRuns = 5;
        int experimentRunRun = 1;
        Integer subtaskNumber = 1;
        Integer taskNumberOfSubtask = 1;

        List resultSuite = null;
        Iterator<Experiment> resultExperimentIterator = null;
        Session session = null;
        TaskInstance taskInstance = null;
        try {
            //create session
            session = HibernateUtil.getSessionFACTORY().openSession();
            session.beginTransaction();

            //create Experiment Suite
            ExperimentSuite experimentSuite = new ExperimentSuite();
            experimentSuite.setName(experimentSuiteName);
            session.save(experimentSuite);

            System system = new System();
            system.setName("flink");
            session.save(system);

            //create Experiment and connect it to ExperimentSuite
            Experiment experiment = new Experiment();
            experiment.setExperimentSuite(experimentSuite);
            experiment.setName(experimentName);
            experiment.setRuns(experimentRuns);
            experiment.setSystem(system);
            system.getExperimentSet().add(experiment);
            session.save(experiment);
            experimentSuite.getExperimentSet().add(experiment);

            //create ExperimentRun and add it to Experiment
            ExperimentRun experimentRun = new ExperimentRun();
            experimentRun.setExperiment(experiment);
            experimentRun.setRun(experimentRunRun);
            session.save(experimentRun);
            experiment.getExperimentRunSet().add(experimentRun);


            //create Task and add it to TaskType and ExperimentRun
            Task task = new Task();
            task.setTaskType("CHAIN");
            task.setExperimentRun(experimentRun);
            task.setNumberOfSubtasks(taskNumberOfSubtask);
            session.save(task);
            experimentRun.getTaskSet().add(task);

            Task task2 = new Task();
            task2.setTaskType("Reduce");
            task2.setExperimentRun(experimentRun);
            task2.setNumberOfSubtasks(taskNumberOfSubtask);
            session.save(task2);
            experimentRun.getTaskSet().add(task2);

            Task task3 = new Task();
            task3.setTaskType("Datasink");
            task3.setExperimentRun(experimentRun);
            task3.setNumberOfSubtasks(taskNumberOfSubtask);
            session.save(task3);
            experimentRun.getTaskSet().add(task3);

            session.getTransaction().commit();
        } catch (Exception e) {
            if (session != null) {
                session.getTransaction().rollback();
            }
            throw e;
        }
        session.close();
    }
    */
}