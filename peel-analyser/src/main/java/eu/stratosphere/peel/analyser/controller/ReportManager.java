package eu.stratosphere.peel.analyser.controller;

import eu.stratosphere.peel.analyser.util.HibernateUtil;
import net.sf.jasperreports.engine.*;
import net.sf.jasperreports.view.JasperViewer;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.spi.SessionFactoryImplementor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by Fabian on 13.11.2014.
 */
public class ReportManager {
    /**
     * Shows a report JasperReports integrated preview mode
     * @param report The file pointing to the Report
     * @throws FileNotFoundException
     * @throws JRException
     * @throws SQLException
     */
    public static void showReport(File report) throws FileNotFoundException, JRException, SQLException {
        JasperReport jasperReport = JasperCompileManager.compileReport(new FileInputStream(report));
        Connection connection = createConnection();
        JasperPrint jasperPrint = JasperFillManager.fillReport(jasperReport, null, connection);
        JasperExportManager.exportReportToHtmlFile(jasperPrint, "H:\\git Projects\\peelanalyser\\src\\main\\resources\\ExperimentRuns.html");
        JasperViewer.viewReport(jasperPrint, false);
        connection.close();
    }

    private static Connection createConnection() throws SQLException {
        Connection connection = null;
        try {
            Class.forName("org.h2.Driver");
        } catch (Exception e){

        }
        Properties properties = new Properties();
        properties.put("user", "");
        properties.put("password", "");
        connection = DriverManager.getConnection("jdbc:h2:./peel-analyser/PeelanalyserDatabase;", properties);
        return connection;
    }
}
