package org.agmip.translators.dssat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import org.agmip.ace.AceBaseComponentType;
import org.agmip.ace.AceDataset;
import org.agmip.ace.AceExperiment;
import org.agmip.ace.AceRecord;
import org.agmip.ace.io.AceParser;
import org.agmip.util.JSONAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DSSAT Observation Data I/O API Class
 *
 * @author Meng Zhang
 * @version 1.0
 */
public class DssatTFileOutput extends DssatCommonOutput {

    private static final Logger LOG = LoggerFactory.getLogger(DssatTFileOutput.class);

    /**
     * DSSAT Time series Observation Data Output method
     *
     * @param arg0 file output path
     * @param result data holder object
     */
    @Override
    public void writeFile(String arg0, Map result) throws IOException {
        AceDataset ace = AceParser.parse(JSONAdapter.toJSON(result));
        write(new File(arg0), ace);
    }

    /**
     * DSSAT Time series Observation Data Output method
     *
     * @param outDir the directory to output the translated files.
     * @param ace the source ACE Dataset
     * @param components subcomponents to translate
     *
     * @return the list of generated files
     */
//    @Override
    public List<File> write(File outDir, AceDataset ace, AceBaseComponentType... components) throws IOException {

        ace.linkDataset();
        List<File> ret = new ArrayList<File>();
        Map<String, List<AceExperiment>> expGroup = groupingExpData(ace);
        String path = revisePath(outDir);

        for (String expName : expGroup.keySet()) {
            writeFile(path, expGroup.get(expName));
            if (outputFile != null) {
                ret.add(outputFile);
            }
        }
        return ret;
    }

    /**
     * DSSAT Time series Observation Data Output method
     *
     * @param arg0 file output path
     * @param exps data holder object
     */
    public void writeFile(String arg0, List<AceExperiment> exps) {

        // Initial variables
        List<AceRecord> obvCol;
        List<List<AceRecord>> observeRecords;    // Array of data holder for time series data
        BufferedWriter bwT;                         // output object
        StringBuilder sbData = new StringBuilder();             // construct the data info in the output
        HashMap<String, String> altTitleList = new HashMap();   // Define alternative fields for the necessary observation data fields; key is necessary field
        // P.S. Add alternative fields here
        HashMap<String, String> titleOutput;                              // contain output data field id
        DssatObservedData obvDataList = new DssatObservedData();    // Varibale list definition

        try {

            // Set default value for missing data
            setDefVal();

            // Get Data from input holder
            observeRecords = new ArrayList();
            for (AceExperiment exp : exps) {
                obvCol = new LinkedList(exp.getOberservedData().getTimeseries());
                if (!obvCol.isEmpty()) {
                    Collections.sort(obvCol, new DssatSortHelper("date"));
                    observeRecords.add(obvCol);
                }
            }
            if (observeRecords.isEmpty()) {
                return;
            }

            // Initial BufferedWriter
            String fileName = getFileName(exps.get(0), "T");
            if (fileName.endsWith(".XXT")) {
                String crid = DssatCRIDHelper.get2BitCrid(getValueOr(exps.get(0), "crid", "XX"));
                fileName = fileName.replaceAll("XX", crid);
            }
            arg0 = revisePath(arg0);
            outputFile = new File(arg0 + fileName);
            bwT = new BufferedWriter(new FileWriter(outputFile));

            // Output Observation File
            // Titel Section
            sbData.append(String.format("*EXP.DATA (T): %1$-10s %2$s\r\n\r\n",
                    fileName.replaceAll("\\.", "").replaceAll("T$", ""),
                    exps.get(0).getValueOr("local_name", defValBlank)));

            titleOutput = new HashMap();
            // Loop all records to find out all the titles
            for (int i = 0; i < observeRecords.size(); i++) {
                for (AceRecord record : observeRecords.get(i)) {

                    // Check if which field is available
                    for (String key : record.keySet()) {
                        // check which optional data is exist, if not, remove from map
                        if (obvDataList.isTimeSeriesData(key)) {
                            titleOutput.put(key, key);

                        } // check if the additional data is too long to output
                        else if (key.toString().length() <= 5) {
                            if (!key.equals("date") && !key.equals("trno")) {
                                titleOutput.put(key, key);
                            }

                        } // If it is too long for DSSAT, give a warning message
                        else {
                            sbError.append("! Waring: Unsuitable data for DSSAT observed data (too long): [").append(key).append("]\r\n");
                        }
                    }

                    // Check if all necessary field is available    // P.S. conrently unuseful
                    for (String title : altTitleList.keySet()) {

                        // check which optional data is exist, if not, remove from map
                        if (getValueOr(record, title, "").equals("")) {

                            if (!getValueOr(record, altTitleList.get(title), "").equals("")) {
                                titleOutput.put(title, altTitleList.get(title));
                            } else {
                                sbError.append("! Waring: Incompleted record because missing data : [").append(title).append("]\r\n");
                            }

                        } else {
                        }
                    }
                }
            }

            // decompress observed data
//            decompressData(observeRecords);
            // Observation Data Section
            String[] titleOutputId = titleOutput.keySet().toArray(new String[0]);
            String pdate = getPdate(exps.get(0));
            for (int i = 0; i < (titleOutputId.length / 39 + titleOutputId.length % 39 == 0 ? 0 : 1); i++) {

                sbData.append("@TRNO   DATE");
                int limit = Math.min(titleOutputId.length, (i + 1) * 39);
                for (int j = i * 39; j < limit; j++) {
                    sbData.append(String.format("%1$6s", titleOutput.get(titleOutputId[j]).toString().toUpperCase()));
                }
                sbData.append("\r\n");

                for (int j = 0; j < observeRecords.size(); j++) {
                    for (AceRecord record : observeRecords.get(j)) {

                        sbData.append(String.format(" %1$5d", j + 1));
                        sbData.append(String.format(" %1$5d", Integer.parseInt(formatDateStr(getValueOr(record, "date", defValI)))));
                        for (int k = i * 39; k < limit; k++) {

                            if (obvDataList.isDapDateType(titleOutputId[k], titleOutput.get(titleOutputId[k]))) {
                                sbData.append(String.format("%1$6s", formatDateStr(pdate, getValueOr(record, titleOutput.get(titleOutputId[k]).toString(), defValI))));
                            } else if (obvDataList.isDateType(titleOutputId[k])) {
                                sbData.append(String.format("%1$6s", formatDateStr(getValueOr(record, titleOutput.get(titleOutputId[k]).toString(), defValI))));
                            } else {
                                sbData.append(" ").append(formatNumStr(5, record, titleOutput.get(titleOutputId[k]), defValI));
                            }

                        }
                        sbData.append("\r\n");
                    }
                }
            }
            // Add section deviding line
            sbData.append("\r\n");

            // Output finish
            bwT.write(sbError.toString());
            bwT.write(sbData.toString());
            bwT.close();
        } catch (IOException e) {
            LOG.error(DssatCommonOutput.getStackTrace(e));
        }
    }
}
