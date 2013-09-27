package org.agmip.translators.dssat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.agmip.ace.AceBaseComponentType;
import org.agmip.ace.AceDataset;
import org.agmip.ace.AceExperiment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DSSAT Run File I/O API Class
 *
 * @author Meng Zhang
 * @version 1.0
 */
public class DssatRunFileOutput extends DssatCommonOutput {

    private static final Logger LOG = LoggerFactory.getLogger(DssatRunFileOutput.class);
    private String dssatVerStr;

    public DssatRunFileOutput() {
        dssatVerStr = null;
    }

    public DssatRunFileOutput(DssatBatchFileOutput.DssatVersion version) {
        dssatVerStr = version.toString();
    }

    /**
     * DSSAT Run Data Output method
     *
     * @param outDir the directory to output the translated files.
     * @param ace the source ACE Dataset
     * @param components subcomponents to translate
     *
     * @return the list of generated files
     */
    @Override
    public List<File> write(File outDir, AceDataset ace, AceBaseComponentType... components) throws IOException {

        ace.linkDataset();
        List<File> ret = new ArrayList<File>();
        String path = revisePath(outDir);
        writeFile(path, ace.getExperiments());
        if (outputFile != null) {
            ret.add(outputFile);
        }
        return ret;
    }

    /**
     * DSSAT Run File Output method
     *
     * @param arg0 file output path
     * @param exps array of data holder object
     */
    private void writeFile(String arg0, List<AceExperiment> exps) {

        // Initial variables
        BufferedWriter bwR;                         // output object

        try {
            // Set default value for missing data
            setDefVal();

            // Get Data from input holder
            if (exps.isEmpty()) {
                return;
            }

            // Get version number
            if (dssatVerStr == null) {
                dssatVerStr = getValueOr(exps.get(0), "crop_model_version", "").replaceAll("\\D", "");
                if (!dssatVerStr.matches("\\d+")) {
                    dssatVerStr = DssatBatchFileOutput.DssatVersion.DSSAT45.toString();
                }
            }

            // Initial BufferedWriter
            arg0 = revisePath(arg0);
            outputFile = new File(arg0 + "Run" + dssatVerStr + ".bat");
            bwR = new BufferedWriter(new FileWriter(outputFile));

            // Output Run File
            bwR.write("C:\\dssat" + dssatVerStr + "\\dscsm0" + dssatVerStr + " b dssbatch.v" + dssatVerStr + "\r\n");
            bwR.write("@echo off\r\n");
            bwR.write("pause\r\n");
            bwR.write("exit\r\n");

            // Output finish
            bwR.close();
        } catch (IOException e) {
            LOG.error(DssatCommonOutput.getStackTrace(e));
        }
    }
}
