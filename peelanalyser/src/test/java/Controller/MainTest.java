package Controller;

import Model.Experiment;
import Util.HibernateUtil;
import org.junit.Test;

import java.util.List;

/**
 * Created by Fabian on 11.11.2014.
 */
public class MainTest {

    @Test
    public void testMain() throws Exception{
        Main.main(new String[]{"--parse", "H:\\TUB Client Synch\\Schule\\Uni\\Bachelorarbeit\\Logs\\Mingliang wally Auswertungen\\results\\pagerank.twitter.wally"});
        //List<Experiment> experimentList = HibernateUtil.getSessionFACTORY().openSession().createQuery("from Experiment").list();
        HibernateUtil.getSessionFACTORY().getCurrentSession().close();

        //ExperimentRun experimentRun = experimentList.get(0).getExperimentRunSet().iterator().next();
        /*Task taskChain =  experimentRun.taskByTaskType("CHAIN");
        TaskInstance taskInstanceChain1 = taskChain.taskInstanceBySubtaskNumber(1);
        TaskInstanceEvents deployed = taskInstanceChain1.getEventByName("to deployed");*/
    }

    @Test
    public void testMainSkipInstances() throws Exception{
        Main.main(new String[]{"--parse", "H:\\TUB Client Synch\\Schule\\Uni\\Bachelorarbeit\\Logs\\Mingliang wally Auswertungen\\tpch3\\results", "--skipInstances"});
        HibernateUtil.getSessionFACTORY().getCurrentSession().close();
        List<Experiment> experimentList = HibernateUtil.getSessionFACTORY().openSession().createQuery("from Experiment").list();
    }
}
