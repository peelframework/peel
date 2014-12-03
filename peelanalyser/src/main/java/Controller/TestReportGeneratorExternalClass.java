package Controller;

import net.sf.jasperreports.engine.*;
import net.sf.jasperreports.engine.data.JRTableModelDataSource;
import net.sf.jasperreports.view.JasperViewer;

import javax.swing.table.DefaultTableModel;

/**
 * Created by ubuntu on 19.10.14.
 */
public class TestReportGeneratorExternalClass {

    /*public static void main(String[] args){
        try {
            JasperReport jr = JasperCompileManager.compileReport("./src/main/resources/ReportExternalClass.jrxml");
            JRTableModelDataSource jrTableModelDataSource = new JRTableModelDataSource(new TestTableModel());
            JasperPrint jasperPrint = JasperFillManager.fillReport(jr, null, jrTableModelDataSource);
            JasperViewer.viewReport(jasperPrint);
        } catch (JRException e) {
            e.printStackTrace();
        }

    }*/
}
