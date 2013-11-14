package org.agmip.translators.dssat;

import java.io.BufferedReader;
import java.io.CharArrayReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.agmip.ace.AceDataset;
import org.agmip.ace.AceExperiment;
import org.agmip.ace.AceObservedData;
import org.agmip.util.JSONAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DSSAT AFile Data I/O API Class
 *
 * @author Meng Zhang
 * @version 1.0
 */
public class DssatAFileInput extends DssatCommonInput {

    private static final Logger LOG = LoggerFactory.getLogger(DssatAFileInput.class);
    public String obvFileKey = "summary";  // P.S. the key name might change
    public String obvDataKey = "data";     // P.S. the key name might change

    /**
     * Constructor with no parameters Set jsonKey as "observed"
     *
     */
    public DssatAFileInput() {
        super();
        jsonKey = "observed";
    }

    /**
     * DSSAT AFile Data input method for Controller using
     *
     * @param brMap The holder for BufferReader objects for all files
     * @return result data holder object
     */
    @Override
    protected HashMap readFile(HashMap brMap) throws IOException {

        HashMap ret = new HashMap();
        ArrayList<HashMap> expArr = new ArrayList<HashMap>();
        HashMap<String, HashMap> files = readObvData(brMap);
//        compressData(file);
        ArrayList<HashMap> obvData;
        HashMap expData;

        for (String exname : files.keySet()) {
            obvData = (ArrayList) files.get(exname).get(obvDataKey);
            for (int i = 0; i < obvData.size(); i++) {
                expData = new HashMap();
                copyItem(expData, files.get(exname), "exname");
                copyItem(expData, files.get(exname), "crid");
                copyItem(expData, files.get(exname), "local_name");
                expData.put(jsonKey, obvData.get(i));
                expArr.add(expData);
            }
        }

        // remove index variables
        ArrayList idNames = new ArrayList();
        idNames.add("trno_a");
        removeIndex(expArr, idNames);
        ret.put("experiments", expArr);

        return ret;
    }

    protected AceDataset readFileToAce(HashMap brMap) throws IOException {
        AceDataset ace = new AceDataset();
//        AceExperiment exp = new AceExperiment();
        readObvData(brMap, ace);
//        ace.addExperiment(exp.rebuildComponent());

        return ace;
    }

    /**
     * DSSAT AFile Data input method for Controller using (return map will not
     * be compressed)
     *
     * @param brMap The holder for BufferReader objects for all files
     * @return result data holder object
     */
    protected HashMap<String, HashMap> readObvData(HashMap brMap) throws IOException {
        AceDataset ace = new AceDataset();
        readObvData(brMap ,ace);
        HashMap<String, HashMap> map = new HashMap();
        for (AceExperiment exp : ace.getExperiments()) {
            String exname = exp.getValueOr("exname", "");
            String json = new String(exp.rebuildComponent(), "UTF-8");
            HashMap data = JSONAdapter.fromJSON(json);
            map.put(exname, data);
        }
        return map;
    }

    protected void readObvData(HashMap brMap, AceDataset ace) throws IOException {

//        HashMap files = new HashMap();
//        HashMap file = new HashMap();
        AceExperiment exp;
        String line;
        HashMap mapA;
        BufferedReader brA = null;
        Object buf;
        LinkedHashMap formats = new LinkedHashMap();
        ArrayList<String> titles = new ArrayList();
        AceObservedData obvData;
        DssatObservedData obvDataList = new DssatObservedData();    // Varibale list definition
        String pdate;

        mapA = (HashMap) brMap.get("A");

        // If AFile File is no been found
        if (mapA.isEmpty()) {
            return;
        }

        for (Object keyA : mapA.keySet()) {

            buf = mapA.get(keyA);
            String fileName = (String) keyA;
//            String exname = fileName.replaceAll("\\.", "").replaceAll("A$", "");
            String baseExname = fileName.replaceAll("\\.\\w\\wA$", "");
            String crid = fileName.replaceAll("\\w+\\.", "").replaceAll("A$", "");
            String local_name = "";
            if (buf instanceof char[]) {
                brA = new BufferedReader(new CharArrayReader((char[]) buf));
            } else {
                brA = (BufferedReader) buf;
            }

//            file = new HashMap();
//            obvData = new ArrayList();

            while ((line = brA.readLine()) != null) {

                // Get content type of line
                judgeContentType(line);

                // Read Observed data
                if (flg[2].equals("data")) {

                    // Read meta info
                    if (flg[0].equals("meta") && flg[1].equals("")) {

                        // Set variables' formats
                        line = line.replaceAll(".*:", "").trim();
//                        formats.clear();
//                        formats.put("null", 10);  // P.S. Since exname in top line is not reliable, read from file name
//                        formats.put("local_name", line.length());
                        // Read line and save into return holder
                        if (line.length() > 10) {
                            local_name = line.substring(10).trim();
                        }
//                        readLine(line, formats, exp);
//                        exp.update("exname", baseExname);
//                        exp.update("crid", DssatCRIDHelper.get3BitCrid(crid));

                    } // Read data info 
                    else {
                        // Set variables' formats
                        formats.clear();
                        for (int i = 0; i < titles.size(); i++) {
                            formats.put(titles.get(i), 6);
                        }
                        // Read line and save into return holder
                        String trno = line.substring(0 ,6).trim();
                        if (trno.equals("")) {
                            LOG.warn("Invliad treatment number found in {}", fileName);
                            continue;
                        }
                        String exname = baseExname + "_" + trno;
                        exp = ace.getExperiment(exname);
                        if (exp != null) {
                            exp = new AceExperiment();
                            exp.update("exname", exname);
                            exp.update("crid", DssatCRIDHelper.get3BitCrid(crid));
                            if (!local_name.equals("")) {
                                exp.update("local_name", local_name);
                            }
                        }
                        obvData = exp.getOberservedData();
                        readLine(line, formats, obvData);
                        pdate = getPdate(exp);
                        for (int i = 0; i < titles.size(); i++) {
                            String title = titles.get(i);
                            if (obvDataList.isDateType(title)) {
                                translateDateStrForDOY(obvData, title, pdate);
                            }
                        }
//                        addToArray(obvData, tmp, "trno_a");
                        obvData.remove("trno_a");
                        exp.getId(true);
                        ace.addExperiment(exp.rebuildComponent());
                    }

                } // Read Observed title
                else if (flg[2].equals("title")) {

                    titles = new ArrayList();
                    line = line.replaceFirst("@", " ");
                    for (int i = 0; i < line.length(); i += 6) {
                        String titleStr = line.substring(i, Math.min(i + 6, line.length())).trim().toLowerCase();
                        if (titleStr.equals("")) {
                            titles.add("null" + i);
                        } else if (titleStr.equals("trno")) {
                            titles.add(titleStr + "_a");
                        } else {
                            titles.add(titleStr);
                        }
                    }

                } else {
                }
            }
//            exp.put(obvDataKey, obvData);
//            files.put(exname, exp);
        }

        brA.close();

//        return files;
    }

    /**
     * Set reading flags for title lines (marked with *)
     *
     * @param line the string of reading line
     */
    @Override
    protected void setTitleFlgs(String line) {
        flg[0] = "meta";
        flg[1] = "";
        flg[2] = "data";
    }
}
