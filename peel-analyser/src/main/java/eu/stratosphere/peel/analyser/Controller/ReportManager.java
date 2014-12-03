package eu.stratosphere.peel.analyser.Controller;

import eu.stratosphere.peel.analyser.Util.HibernateUtil;
import net.sf.jasperreports.engine.*;
import net.sf.jasperreports.view.JasperViewer;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.spi.SessionFactoryImplementor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by Fabian on 13.11.2014.
 */
public class ReportManager {
    public static void showReport(File report) throws FileNotFoundException, JRException, SQLException {
        JasperReport jasperReport = JasperCompileManager.compileReport(new FileInputStream(report));
        SessionFactoryImplementor sessionFactoryImplementor = (SessionFactoryImplementor)HibernateUtil.getSessionFACTORY();
        ConnectionProvider connectionProvider = sessionFactoryImplementor.getConnectionProvider();
        Connection connection = connectionProvider.getConnection();
        JasperPrint jasperPrint = JasperFillManager.fillReport(jasperReport, null, getConnection());
        JasperExportManager.exportReportToHtmlFile(jasperPrint, "H:\\git Projects\\peelanalyser\\src\\main\\resources\\ExperimentRuns.html");
        JasperViewer.viewReport(jasperPrint, false);
        int a = 5;
    }

    private static Connection getConnection() throws SQLException {
        SessionFactoryImplementor sessionFactoryImplementor = (SessionFactoryImplementor)HibernateUtil.getSessionFACTORY();
        ConnectionProvider connectionProvider = sessionFactoryImplementor.getConnectionProvider();
        return connectionProvider.getConnection();
    }
}
