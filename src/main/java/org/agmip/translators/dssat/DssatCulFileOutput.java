package org.agmip.translators.dssat;

import com.google.common.hash.HashCode;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.agmip.ace.AceBaseComponentType;
import org.agmip.ace.AceComponent;
import org.agmip.ace.AceDataset;
import org.agmip.ace.AceExperiment;
import org.agmip.ace.AceRecord;
import org.agmip.ace.AceRecordCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DSSAT Cultivar Data I/O API Class
 *
 * @author Meng Zhang
 * @version 1.0
 */
public class DssatCulFileOutput extends DssatCommonOutput implements DssatXATFileOutputI {

    private static final Logger LOG = LoggerFactory.getLogger(DssatCulFileOutput.class);
    private List<File> fileArr;

    
    /**
     * DSSAT Cultivar Data Output method
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
        Map<String, List<AceExperiment>> expGroup = groupingExpData(ace);
        String path = revisePath(outDir);
        fileArr = new ArrayList();

        for (String expName : expGroup.keySet()) {
            writeFile(path, expGroup.get(expName));
            if (outputFile != null) {
                ret.add(outputFile);
            }
        }
        return ret;
    }

    /**
     * DSSAT Experiment Data Output method
     *
     * @param arg0 file output path
     * @param exps the list of ACE experiment data
     */
    @Override
    public void writeFile(String arg0, List<AceExperiment> exps) {

        // Initial variables
        AceComponent culData;              // Data holder for one site of cultivar data
        AceRecordCollection culArr;    // Data holder for one site of cultivar data
        BufferedWriter bwC;                             // output object
        StringBuilder sbData = new StringBuilder();     // construct the data info in the output

        try {

            // Set default value for missing data
            setDefVal();
            
            if (exps.isEmpty()) {
                return;
            }

            // Initial BufferedWriter
            // Get File name
            String fileName = getFileName(exps.get(0), "X");
            if (fileName.matches("TEMP\\d{4}\\.\\w{2}X")) {
                fileName = "Cultivar.CUL";
            } else {
                try {
                    fileName = fileName.replaceAll("\\.", "_") + ".CUL";
                } catch (Exception e) {
                    fileName = "Cultivar.CUL";
                }
            }
            arg0 = revisePath(arg0);
            outputFile = new File(arg0 + fileName);
            bwC = new BufferedWriter(new FileWriter(outputFile, fileArr.contains(outputFile)));

            // Output Cultivar File
            String lastHeaderInfo = "";
            String lastTitles = "";
            HashSet<HashCode> culHashs = new HashSet();
            for (AceExperiment exp : exps) {
                culData = exp.getSubcomponent("dssat_cultivar_info");
                culArr = culData.getRecords("data");
                if (culArr.isEmpty()) {
                    continue;
                }
                for (Iterator<AceRecord> it = culArr.iterator(); it.hasNext();) {
                    AceRecord culRecord = it.next();
                    HashCode culHash = culRecord.getRawComponentHash();
                    if (culHashs.contains(culHash)) {
                        continue;
                    } else {
                        culHashs.add(culHash);
                    }
                    // If come to new header, add header line and title line
                    if (!getValueOr(culRecord, "header_info", "").equals(lastHeaderInfo)) {
                        lastHeaderInfo = getValueOr(culRecord, "header_info", "");
                        sbData.append(lastHeaderInfo).append("\r\n");
                        lastTitles = getValueOr(culRecord, "cul_titles", "");
                        sbData.append(lastTitles).append("\r\n");
                    }
                    // If come to new title line, add title line
                    if (!getValueOr(culRecord, "cul_titles", "").equals(lastTitles)) {
                        lastTitles = getValueOr(culRecord, "cul_titles", "");
                        sbData.append(lastTitles).append("\r\n");
                    }
                    // Write data line
                    sbData.append(getValueOr(culRecord, "cul_info", "")).append("\r\n");

                }
            }

            // Output finish
            bwC.write(sbError.toString());
            bwC.write(sbData.toString());
            bwC.close();

        } catch (IOException e) {
            LOG.error(DssatCommonOutput.getStackTrace(e));
        }
    }
}
