package org.agmip.translators.dssat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.agmip.ace.AceBaseComponentType;
import org.agmip.ace.AceDataset;
import org.agmip.ace.AceExperiment;
import org.agmip.ace.AceRecord;
import org.agmip.ace.AceRecordCollection;
import org.agmip.ace.AceSoil;
import org.agmip.ace.io.AceParser;
import org.agmip.util.JSONAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DSSAT Soil Data I/O API Class
 *
 * @author Meng Zhang
 * @version 1.0
 */
public class DssatSoilOutput extends DssatCommonOutput {

    private static final Logger LOG = LoggerFactory.getLogger(DssatSoilOutput.class);

    /**
     * DSSAT Soil Data Output method
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
     * DSSAT Soil Data Output method
     *
     * @param outDir the directory to ouput the translated files.
     * @param ace the source ACE Dataset
     * @param components subcomponents to translate
     *
     * @return the list of generated files
     */
//    @Override
    public List<File> write(File outDir, AceDataset ace, AceBaseComponentType... components) {

        List<File> ret = new ArrayList<File>();
        Map<String, String> comments = new HashMap();
        Map<String, List<AceSoil>> soilGroup = groupingSoilData(ace, comments);
        String path = revisePath(outDir);

        for (String fileName : soilGroup.keySet()) {
            File newFile = writeFile(path, soilGroup.get(fileName), comments);
            if (newFile != null) {
                ret.add(newFile);
            }
        }
        return ret;
    }

    /**
     * Group the soil site data
     *
     * The soil data in the same group must contain same first two letters in
     * the SOIL_ID
     *
     * @param ace the ACE data set
     * @param commnets will record the comments related with soil site during grouping process
     *
     * @return the map of grouped soil data
     */
    protected Map<String, List<AceSoil>> groupingSoilData(AceDataset ace, Map<String, String> commnets) {
        Map<String, List<AceSoil>> soilGroup = new HashMap();
        List<AceExperiment> exps = ace.getExperiments();

        if (exps.isEmpty()) {
            for (AceSoil soil : ace.getSoils()) {
                setGroup(soilGroup, soil);
            }
        } else {
            for (AceExperiment exp : exps) {
                AceSoil soil = exp.getSoil();
                if (soil != null) {
                    setGroup(soilGroup, soil);

                    String soil_id = getValueOr(soil, "soil_id", "");
                    if (commnets.containsKey(soil_id)) {
                        commnets.put(soil_id, commnets.get(soil_id) + "," + getValueOr(exp, "exname", "N/A"));
                    } else {
                        commnets.put(soil_id, getValueOr(exp, "exname", "N/A"));
                    }
                }
            }
        }
        return soilGroup;
    }

    private void setGroup(Map<String, List<AceSoil>> soilGroup, AceSoil soil) {
        String soil_id = getValueOr(soil, "soil_id", "");
        String fileName;
        if (soil_id.length() < 2) {
            fileName = "";
        } else {
            fileName = soil_id.substring(0, 2);
        }

        if (soilGroup.containsKey(fileName)) {
            soilGroup.get(fileName).add(soil);
        } else {
            ArrayList<AceSoil> arr = new ArrayList();
            arr.add(soil);
            soilGroup.put(fileName, arr);
        }
    }

    /**
     * DSSAT Soil Data Output method
     *
     * All the soil site data will be output into one file, which means all the
     * site should have the same first two letters in the SOIL_ID
     *
     * @param arg0 file output path
     * @param soils data holder object
     * @param comments the experiment information related with soil site, used
     * for output comments
     *
     * @return the generated soil file
     */
    public File writeFile(String arg0, List<AceSoil> soils, Map<String, String> comments) {

        // Initial variables
        AceSoil soilSite;                       // Data holder for one site of soil data
        AceRecordCollection soilLayers;                          // Soil layer data array
        AceRecord soilLayer;                     // Data holder for one layer data
        BufferedWriter bwS;                             // output object
        StringBuilder sbTitle = new StringBuilder();
        StringBuilder sbSites = new StringBuilder();
        StringBuilder sbData;                           // construct the data info in the output
        StringBuilder sbLyrP2 = new StringBuilder();    // output string for second part of layer data
        boolean p2Flg;
        String[] p2Ids = {"slpx", "slpt", "slpo", "caco3", "slal", "slfe", "slmn", "slbs", "slpa", "slpb", "slke", "slmg", "slna", "slsu", "slec", "slca"};
        if (comments == null) {
            comments = new HashMap();
        }

        try {

            // Set default value for missing data
            setDefVal();

            if (soils.isEmpty()) {
                return outputFile;
            }
            soilSite = soils.get(0);

            // Initial BufferedWriter
            // Get File name
            String soilId = soilSite.getValueOr("soil_id", "");
            String fileName;
            if (soilId.equals("")) {
                fileName = "soil.SOL";
            } else {
                try {
                    fileName = soilId.substring(0, 2) + ".SOL";
                } catch (Exception e) {
                    fileName = "soil.SOL";
                }

            }
            arg0 = revisePath(arg0);
            outputFile = new File(arg0 + fileName);
//                boolean existFlg = outputFile.exists();
            bwS = new BufferedWriter(new FileWriter(outputFile));

            // Description info for output by translator
            sbTitle.append("!This soil file is created by DSSAT translator tool on ").append(Calendar.getInstance().getTime()).append(".\r\n");
            sbTitle.append("*SOILS: ");

            for (int i = 0; i < soils.size(); i++) {

                soilSite = soils.get(i);
                sbError = new StringBuilder();
                sbData = new StringBuilder();

                // Output Soil File
                // Titel Section
                String sl_notes = soilSite.getValueOr("sl_notes", defValBlank);
                if (!sl_notes.equals(defValBlank)) {
                    sbTitle.append(sl_notes).append("; ");
                }
                sbData.append("!The ACE ID is ").append(soilSite.getId()).append(".\r\n");
                sbData.append("!This soil data is used for the experiment of ").append(getValueOr(comments, soilSite.getValueOr("soil_id", ""), "N/A")).append(".\r\n!\r\n");

                // Site Info Section
                String soil_id = getSoilID(soilSite);
                if (soil_id.equals("")) {
                    sbError.append("! Warning: Incompleted record because missing data : [soil_id]\r\n");
                } else if (soil_id.length() > 10) {
                    sbError.append("! Warning: Oversized data : [soil_id] ").append(soil_id).append("\r\n");
                }
                sbData.append(String.format("*%1$-10s  %2$-11s %3$-5s %4$5s %5$s\r\n",
                        soil_id,
                        formatStr(11, soilSite, "sl_source", defValC),
                        formatStr(5, transSltx(soilSite.getValueOr("sltx", defValC)), "sltx"),
                        formatNumStr(5, soilSite, "sldp", defValR),
                        soilSite.getValueOr("soil_name", defValC).toString()));
                sbData.append("@SITE        COUNTRY          LAT     LONG SCS FAMILY\r\n");
                sbData.append(String.format(" %1$-11s %2$-11s %3$9s%4$8s %5$s\r\n",
                        formatStr(11, soilSite, "sl_loc_3", defValC),
                        formatStr(11, soilSite, "sl_loc_1", defValC),
                        formatNumStr(8, soilSite, "soil_lat", defValR), // P.S. Definition changed 9 -> 10 (06/24)
                        formatNumStr(8, soilSite, "soil_long", defValR), // P.S. Definition changed 9 -> 8  (06/24)
                        soilSite.getValueOr("classification", defValC).toString()));
                sbData.append("@ SCOM  SALB  SLU1  SLDR  SLRO  SLNF  SLPF  SMHB  SMPX  SMKE\r\n");
//                if (getObjectOr(soilSite, "slnf", "").equals("")) {
//                    sbError.append("! Warning: missing data : [slnf], and will automatically use default value '1'\r\n");
//                }
//                if (getObjectOr(soilSite, "slpf", "").equals("")) {
//                    sbError.append("! Warning: missing data : [slpf], and will automatically use default value '0.92'\r\n");
//                }
                sbData.append(String.format(" %1$5s %2$5s %3$5s %4$5s %5$5s %6$5s %7$5s %8$5s %9$5s %10$5s\r\n",
                        soilSite.getValueOr("sscol", defValC).toString(),
                        formatNumStr(5, soilSite, "salb", defValR),
                        formatNumStr(5, soilSite, "slu1", defValR),
                        formatNumStr(5, soilSite, "sldr", defValR),
                        formatNumStr(5, soilSite, "slro", defValR),
                        formatNumStr(5, soilSite, "slnf", defValR), // P.S. Remove default value as '1'
                        formatNumStr(5, soilSite, "slpf", defValR), // P.S. Remove default value as '0.92'
                        soilSite.getValueOr("smhb", defValC).toString(),
                        soilSite.getValueOr("smpx", defValC).toString(),
                        soilSite.getValueOr("smke", defValC).toString()));

                // Soil Layer data section
                soilLayers = soilSite.getSoilLayers();
                // part one
                sbData.append("@  SLB  SLMH  SLLL  SDUL  SSAT  SRGF  SSKS  SBDM  SLOC  SLCL  SLSI  SLCF  SLNI  SLHW  SLHB  SCEC  SADC\r\n");
                // part two
                sbLyrP2.append("@  SLB  SLPX  SLPT  SLPO CACO3  SLAL  SLFE  SLMN  SLBS  SLPA  SLPB  SLKE  SLMG  SLNA  SLSU  SLEC  SLCA\r\n");
                p2Flg = false;

                // Loop for laryer data
                for (Iterator<AceRecord> it = soilLayers.iterator(); it.hasNext();) {
                    soilLayer = it.next();
                    // part one
                    sbData.append(String.format(" %1$5s %2$5s %3$5s %4$5s %5$5s %6$5s %7$5s %8$5s %9$5s %10$5s %11$5s %12$5s %13$5s %14$5s %15$5s %16$5s %17$5s\r\n",
                            formatNumStr(5, soilLayer, "sllb", defValR),
                            soilLayer.getValueOr("slmh", defValC),
                            formatNumStr(5, soilLayer, "slll", defValR),
                            formatNumStr(5, soilLayer, "sldul", defValR),
                            formatNumStr(5, soilLayer, "slsat", defValR),
                            formatNumStr(5, soilLayer, "slrgf", defValR),
                            formatNumStr(5, soilLayer, "sksat", defValR),
                            formatNumStr(5, soilLayer, "slbdm", defValR),
                            formatNumStr(5, soilLayer, "sloc", defValR),
                            formatNumStr(5, soilLayer, "slcly", defValR),
                            formatNumStr(5, soilLayer, "slsil", defValR),
                            formatNumStr(5, soilLayer, "slcf", defValR),
                            formatNumStr(5, soilLayer, "slni", defValR),
                            formatNumStr(5, soilLayer, "slphw", defValR),
                            formatNumStr(5, soilLayer, "slphb", defValR),
                            formatNumStr(5, soilLayer, "slcec", defValR),
                            formatNumStr(5, soilLayer, "sladc", defValR)));

                    // part two
                    sbLyrP2.append(String.format(" %1$5s %2$5s %3$5s %4$5s %5$5s %6$5s %7$5s %8$5s %9$5s %10$5s %11$5s %12$5s %13$5s %14$5s %15$5s %16$5s %17$5s\r\n",
                            formatNumStr(5, soilLayer, "sllb", defValR),
                            formatNumStr(5, soilLayer, "slpx", defValR),
                            formatNumStr(5, soilLayer, "slpt", defValR),
                            formatNumStr(5, soilLayer, "slpo", defValR),
                            formatNumStr(5, soilLayer, "caco3", defValR), // P.S. Different with document (DSSAT vol2.pdf)
                            formatNumStr(5, soilLayer, "slal", defValR),
                            formatNumStr(5, soilLayer, "slfe", defValR),
                            formatNumStr(5, soilLayer, "slmn", defValR),
                            formatNumStr(5, soilLayer, "slbs", defValR),
                            formatNumStr(5, soilLayer, "slpa", defValR),
                            formatNumStr(5, soilLayer, "slpb", defValR),
                            formatNumStr(5, soilLayer, "slke", defValR),
                            formatNumStr(5, soilLayer, "slmg", defValR),
                            formatNumStr(5, soilLayer, "slna", defValR),
                            formatNumStr(5, soilLayer, "slsu", defValR),
                            formatNumStr(5, soilLayer, "slec", defValR),
                            formatNumStr(5, soilLayer, "slca", defValR)));

                    // Check if there is 2nd part of layer data for output
                    if (!p2Flg) {
                        for (int j = 0; j < p2Ids.length; j++) {
                            if (!soilLayer.getValueOr(p2Ids[j], "").equals("")) {
                                p2Flg = true;
                                break;
                            }
                        }
                    }
                }

                // Add part two
                if (p2Flg) {
                    sbData.append(sbLyrP2.toString()).append("\r\n");
                    sbLyrP2 = new StringBuilder();
                } else {
                    sbData.append("\r\n");
                }

                // Finish one site
                sbSites.append(sbError.toString());
                sbSites.append(sbData.toString());
            }

            // Output finish
            sbTitle.append("\r\n\r\n");
            bwS.write(sbTitle.toString());
            bwS.write(sbSites.toString());
            bwS.close();

        } catch (IOException e) {
            LOG.error(DssatCommonOutput.getStackTrace(e));
        }
        return outputFile;
    }
}
