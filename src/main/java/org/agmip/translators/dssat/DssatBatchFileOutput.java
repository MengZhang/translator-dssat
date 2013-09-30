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
import org.agmip.ace.LookupCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DSSAT Batch File I/O API Class
 *
 * @author Meng Zhang
 * @version 1.0
 */
public class DssatBatchFileOutput extends DssatCommonOutput {

    private static final Logger LOG = LoggerFactory.getLogger(DssatBatchFileOutput.class);
    private String dssatVerStr;

    public enum DssatVersion {

        DSSAT45 {
            @Override
            public String toString() {
                return "45";
            }
        },
        DSSAT46 {
            @Override
            public String toString() {
                return "46";
            }
        }
    }

    public DssatBatchFileOutput() {
        dssatVerStr = null;
    }

    public DssatBatchFileOutput(DssatVersion version) {
        dssatVerStr = version.toString();
    }

    /**
     * DSSAT Batch Data Output method
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
     * DSSAT Batch File Output method
     *
     * @param arg0 file output path
     * @param exps array of data holder object
     */
    private void writeFile(String arg0, List<AceExperiment> exps) {

        // Initial variables
        AceExperiment exp;                       // Data holder for summary data
        BufferedWriter bwB;                         // output object
        StringBuilder sbData = new StringBuilder(); // construct the data info in the output

        try {

            // Set default value for missing data
            setDefVal();

            // Get Data from input holder
            if (exps.isEmpty()) {
                return;
            }
            exp = exps.get(0);

            // Get version number
            if (dssatVerStr == null) {
                dssatVerStr = getValueOr(exp, "crop_model_version", "").replaceAll("\\D", "");
                if (!dssatVerStr.matches("\\d+")) {
                    dssatVerStr = DssatVersion.DSSAT45.toString();
                }
            }
            // Initial BufferedWriter
            arg0 = revisePath(arg0);
            outputFile = new File(arg0 + "DSSBatch.v" + dssatVerStr);
            bwB = new BufferedWriter(new FileWriter(outputFile));

            // Output Batch File
            // Titel Section
            String crop = getCropName(exp);
            String dssatPath = "C:\\DSSAT" + dssatVerStr + "\\";
            String exFileName = getFileName(exp, "X");
            int expNo = exps.size();

            // Write title section
            sbData.append("$BATCH(").append(crop.toUpperCase()).append(")\r\n!\r\n");
            sbData.append(String.format("! Command Line : %1$sDSCS0M%2$s.EXE B DSSBatch.v%2$s\r\n", dssatPath, dssatVerStr));
            sbData.append("! Crop         : ").append(crop).append("\r\n");
            sbData.append("! Experiment   : ").append(exFileName).append("\r\n");
            sbData.append("! ExpNo        : ").append(expNo).append("\r\n");
            sbData.append(String.format("! Debug        : %1$sDSCSM0%2$s.EXE \" B DSSBatch.v%2$s\"\r\n!\r\n", dssatPath, dssatVerStr));

            // Body Section
            sbData.append("@FILEX                                                                                        TRTNO     RP     SQ     OP     CO\r\n");
            for (int i = 0; i < exps.size(); i++) {
                exp = exps.get(i);
                exFileName = getFileName(exp, "X");

                // Get DSSAT Sequence info
                // TODO will be update later
//                HashMap dssatSeqData = getObjectOr(exp, "dssat_sequence", new HashMap());
//                ArrayList<HashMap> dssatSeqArr = getObjectOr(dssatSeqData, "data", new ArrayList<HashMap>());
//                // If missing, set default value
//                if (dssatSeqArr.isEmpty()) {
//                    HashMap tmp = new HashMap();
//                    tmp.put("sq", "1");
//                    tmp.put("op", "1");
//                    tmp.put("co", "0");
//                    dssatSeqArr.add(tmp);
//                }
                // Output all info
//                for (int j = 0; j < dssatSeqArr.size(); j++) {
                sbData.append(String.format("%1$-92s %2$6d %3$6s %4$6s %5$6s %6$6s\r\n",
                        //                            folderPath + exFileName,
                        exFileName,
                        i + 1, //getObjectOr(dssatSeqArr.get(j), "trno", "1"),
                        "1",
                        "1", //getObjectOr(dssatSeqArr.get(j), "sq", "1"),
                        "1", //getObjectOr(dssatSeqArr.get(j), "op", "1"),
                        "0" //getObjectOr(dssatSeqArr.get(j), "co", "0")
                        ));
//                }
            }

            // Output finish
            bwB.write(sbError.toString());
            bwB.write(sbData.toString());
            bwB.close();
        } catch (IOException e) {
            LOG.error(DssatCommonOutput.getStackTrace(e));
        }

    }

    /**
     * Get crop name string
     *
     * @param exp experiment data holder
     * @return
     */
    private String getCropName(AceExperiment exp) {
        String ret;
        String crid;

        // Get crop id
        crid = getCrid(exp);

        // Get crop name string
        ret = LookupCodes.lookupCode("CRID", crid, "common", "DSSAT");
        if (ret.equals(crid)) {
            ret = "Unkown";
            sbError.append("! Warning: Undefined crop id: [").append(crid).append("]\r\n");
        }

//        // Get crop name string
//        if ("BH".equals(crid)) {
//            ret = "Bahia";
//        } else if ("BA".equals(crid)) {
//            ret = "Barley";
//        } else if ("BR".equals(crid)) {
//            ret = "Brachiaria";
//        } else if ("CB".equals(crid)) {
//            ret = "Cabbage";
//        } else if ("CS".equals(crid)) {
//            ret = "Cassava";
//        } else if ("CH".equals(crid)) {
//            ret = "Chickpea";
//        } else if ("CO".equals(crid)) {
//            ret = "Cotton";
//        } else if ("CP".equals(crid)) {
//            ret = "Cowpea";
//        } else if ("BN".equals(crid)) {
//            ret = "Drybean";
//        } else if ("FB".equals(crid)) {
//            ret = "FabaBean";
//        } else if ("FA".equals(crid)) {
//            ret = "Fallow";
//        } else if ("GB".equals(crid)) {
//            ret = "GreenBean";
//        } else if ("MZ".equals(crid)) {
//            ret = "Maize";
//        } else if ("ML".equals(crid)) {
//            ret = "Millet";
//        } else if ("PN".equals(crid)) {
//            ret = "Peanut";
//        } else if ("PR".equals(crid)) {
//            ret = "Pepper";
//        } else if ("PI".equals(crid)) {
//            ret = "PineApple";
//        } else if ("PT".equals(crid)) {
//            ret = "Potato";
//        } else if ("RI".equals(crid)) {
//            ret = "Rice";
//        } else if ("SG".equals(crid)) {
//            ret = "Sorghum";
//        } else if ("SB".equals(crid)) {
//            ret = "Soybean";
//        } else if ("SC".equals(crid)) {
//            ret = "Sugarcane";
//        } else if ("SU".equals(crid)) {
//            ret = "Sunflower";
//        } else if ("SW".equals(crid)) {
//            ret = "SweetCorn";
//        } else if ("TN".equals(crid)) {
//            ret = "Tanier";
//        } else if ("TR".equals(crid)) {
//            ret = "Taro";
//        } else if ("TM".equals(crid)) {
//            ret = "Tomato";
//        } else if ("VB".equals(crid)) {
//            ret = "Velvetbean";
//        } else if ("WH".equals(crid)) {
//            ret = "Wheat";
//        } else if ("SQ".equals(crid)) {
//            ret = "Sequence";
//        } else {
//            ret = "Unkown";
//            sbError.append("! Warning: Undefined crop id: [").append(crid).append("]\r\n");
//        }
        return ret;
    }
}
