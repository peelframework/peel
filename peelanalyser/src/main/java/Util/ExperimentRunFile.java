package Util;

import Model.ExperimentRun;

import java.io.File;

/**
 * Created by ubuntu on 08.11.14.
 */
public class ExperimentRunFile {
    private ExperimentRun experimentRun;
    private File file;

    public ExperimentRunFile(ExperimentRun experimentRun, File file){
        this.experimentRun = experimentRun;
        this.file = file;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public ExperimentRun getExperimentRun() {
        return experimentRun;
    }

    public void setExperimentRun(ExperimentRun experimentRun) {
        this.experimentRun = experimentRun;
    }
}
