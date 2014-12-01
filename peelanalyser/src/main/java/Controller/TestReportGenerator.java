package Controller;

import net.sf.jasperreports.engine.*;
import net.sf.jasperreports.engine.data.JRTableModelDataSource;
import net.sf.jasperreports.engine.design.JasperDesign;
import net.sf.jasperreports.view.JasperViewer;

import javax.swing.table.DefaultTableModel;

/**
 * Created by ubuntu on 19.10.14.
 */
public class TestReportGenerator {
    /*public static void main(String[] args){
        try {
            String[] cols = {"OrderID", "Total"};
            DefaultTableModel dtm = new DefaultTableModel(cols, 0);

            dtm.addRow(new Object[]{"OD001", 1200.0D});
            dtm.addRow(new Object[]{"0D002", 1300.0D});
            dtm.addRow(new Object[]{"0D003", 900.0D});

            JRTableModelDataSource jrTableModelDataSource = new JRTableModelDataSource(dtm);
            JasperReport jr = JasperCompileManager.compileReport("./src/main/resources/TestReport.jrxml");
            JasperPrint jp = JasperFillManager.fillReport(jr, null, jrTableModelDataSource);
            JasperViewer.viewReport(jp);

            /*JRTableModelDataSource jrTableModelDataSource = new JRTableModelDataSource(new TestTableModel());
            JasperPrint jasperPrint = JasperFillManager.fillReport(jr, null, jrTableModelDataSource);
            JasperViewer.viewReport(jasperPrint);
        } catch (JRException e) {
            e.printStackTrace();
        }
    }
        */
}
