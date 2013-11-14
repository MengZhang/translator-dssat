package org.agmip.translators.dssat;

import java.io.BufferedReader;
import java.io.CharArrayReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import org.agmip.ace.AceDataset;
import org.agmip.ace.AceRecord;
import org.agmip.ace.AceRecordCollection;
import org.agmip.ace.AceWeather;
import org.agmip.common.Functions;
import org.agmip.util.JSONAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DSSAT Weather Data I/O API Class
 *
 * @author Meng Zhang
 * @version 1.0
 */
public class DssatWeatherInput extends DssatCommonInput {

    private static final Logger LOG = LoggerFactory.getLogger(DssatWeatherInput.class);

    /**
     * Constructor with no parameters Set jsonKey as "weather"
     *
     */
    public DssatWeatherInput() {
        super();
        jsonKey = "weather";
    }

    /**
     * DSSAT Weather Data input method for only inputing weather file
     *
     * @param brMap The holder for BufferReader objects for all files
     * @return result data holder object
     */
    @Override
    protected HashMap readFile(HashMap brMap) throws IOException {
        HashMap ret = new HashMap();
        ArrayList<HashMap> files = readDailyData(brMap, new HashMap());
//        compressData(files);
        ret.put("weathers", files);

        return ret;
    }

    protected AceDataset readFileToAce(HashMap brMap) throws IOException {
        AceDataset ace = new AceDataset();
        ArrayList<AceWeather> wths = readDailyData(brMap);
        for (AceWeather wth : wths) {
            ace.addWeather(wth.rebuildComponent());
        }

        return ace;
    }

    /**
     * DSSAT Weather Data input method for Controller using (return value will
     * not be compressed)
     *
     * @param brMap The holder for BufferReader objects for all files
     * @return result data holder object
     */
    protected ArrayList<HashMap> readDailyData(HashMap brMap, HashMap ret) throws IOException {
        ArrayList<AceWeather> wths = readDailyData(brMap);
        ArrayList<HashMap> arr = new ArrayList();
        for (AceWeather wth : wths) {
            String json = new String(wth.rebuildComponent(), "UTF-8");
            HashMap data = JSONAdapter.fromJSON(json);
            arr.add((HashMap) data);
        }
        return arr;
    }

    protected ArrayList<AceWeather> readDailyData(HashMap brMap) throws IOException {

        AceWeather wth;
        AceRecordCollection daily;
        ArrayList titles;
        String line;
        BufferedReader brW = null;
        Object buf;
        HashMap mapW;
        LinkedHashMap formats = new LinkedHashMap();
        HashMap<String, AceWeather> wths = new HashMap();
        String fileName;

        mapW = (HashMap) brMap.get("W");

        // If Weather File is no been found
        if (mapW.isEmpty()) {
            return new ArrayList();
        }

        for (Object key : mapW.keySet()) {

            fileName = (String) key;
            if (fileName.lastIndexOf(".") > 0) {
                fileName = fileName.substring(0, fileName.lastIndexOf("."));
            }
            String wst_id;
            String clim_id;
            if (fileName.length() == 8 && !fileName.matches("\\w{4}\\d{4}$")) {
                wst_id = fileName;
                clim_id = fileName.substring(4);
            } else {
                wst_id = fileName.substring(0, 4);
                clim_id = "0XXX";
            }

            buf = mapW.get(key);
            if (buf instanceof char[]) {
                brW = new BufferedReader(new CharArrayReader((char[]) buf));
            } else {
                brW = (BufferedReader) buf;
            }
            wth = new AceWeather();
            daily = wth.getDailyWeather();
            titles = new ArrayList();

            while ((line = brW.readLine()) != null) {

                // Get content type of line
                judgeContentType(line);

                // Read Weather File Info
                if (flg[0].equals("weather") && flg[1].equals("") && flg[2].equals("data")) {

                    // header info
                    wth.update("wst_notes", line.replaceFirst("\\*[Ww][Ee][Aa][Tt][Hh][Ee][Rr]\\s*([Dd][Aa][Tt][Aa]\\s*)*:?", "").trim());

                } // Read Weather Data
                else if (flg[2].equals("data")) {

                    // Weather station info
                    if (flg[1].contains("insi ")) {

                        // Set variables' formats
                        formats.clear();
                        String[] names = flg[1].split("\\s+");
                        for (int i = 0; i < names.length; i++) {
                            if (names[i].equalsIgnoreCase("INSI")) {
                                formats.put("dssat_insi", 6);
                            } else if (names[i].equalsIgnoreCase("LAT")) {
                                formats.put("wst_lat", 9);
                            } else if (names[i].equalsIgnoreCase("LONG")) {
                                formats.put("wst_long", 9);
                            } else if (names[i].equalsIgnoreCase("ELEV")) {
                                formats.put("wst_elev", 6);
                            } else if (names[i].equalsIgnoreCase("TAV")) {
                                formats.put("tav", 6);
                            } else if (names[i].equalsIgnoreCase("AMP")) {
                                formats.put("tamp", 6);
                            } else if (names[i].equalsIgnoreCase("REFHT")) {
                                formats.put("refht", 6);
                            } else if (names[i].equalsIgnoreCase("WNDHT")) {
                                formats.put("wndht", 6);
                            } else if (names[i].equalsIgnoreCase("CO2")) {
                                formats.put("co2y", 6);
                            } else if (names[i].equalsIgnoreCase("CCO2")) {
                                formats.put("co2y", 6);
                            } else {
                                int end = flg[1].indexOf(names[i]) + names[i].length();
                                int start = 0;
                                if (i > 0) {
                                    start = flg[1].indexOf(names[i - 1]) + names[i - 1].length();
                                }
                                formats.put(names[i].toLowerCase(), end - start);
                            }
                        }

                        // Read line and save into return holder
                        readLine(line, formats, wth);
                        // Check if the WST_ID inside file is matching with the file name
                        String dssat_insi = wth.getValue("dssat_insi");
                        if (dssat_insi == null || !wst_id.startsWith(dssat_insi)) {
                            LOG.warn("The name of weather file [{}] does not match with the INSI ({}) in the file.", key, dssat_insi);
                        }
//                        if (wst_name != null) {
//                            file.put("wst_name", fileName + " " + wst_name);
//                        } else {
//                            file.put("wst_name", fileName);
//                        }

                    } // Weather daily data
                    else if (flg[1].startsWith("date ")) {

                        // Set variables' formats
                        formats.clear();
                        formats.put("w_date", 5);
                        for (int i = 0; i < titles.size(); i++) {
                            formats.put(titles.get(i), 6);
                        }
                        // Read line and save into return holder
                        AceRecord tmp = new AceRecord();
                        readLine(line, formats, tmp);
                        // translate date from yyddd format to yyyymmdd format
                        translateDateStr(tmp, "w_date");
                        daily.add(tmp);

                    } else {
                    }
                } // Data Title Info
                else if (flg[2].equals("title")) {
                    // Dialy Data Title
                    if (flg[1].startsWith("date ")) {
                        for (int i = 6; i < line.length(); i += 6) {
                            String title = line.substring(i, Math.min(i + 6, line.length())).trim();
                            if (title.equalsIgnoreCase("DEWP")) {
                                titles.add("tdew");
                            } else if (title.equalsIgnoreCase("PAR")) {
                                titles.add("pard");
                            } else if (title.equalsIgnoreCase("VPRS")) {
                                titles.add("vprsd");
                            } else if (title.equalsIgnoreCase("RHUM")) {
                                titles.add("rhumd");
                            } else if (title.equals("")) {
                                titles.add("null" + i);
                            } else {
                                titles.add(title.toLowerCase());
                            }
                        }
                    } else {
                    }
//                } else if (flg[2].equals("comment")) {
//                    if (line.startsWith("!Climate ID: ")) {
//                        clim_id = line.substring(13).trim();
//                        if (clim_id.toUpperCase().equals("N/A")) {
//                            clim_id = "";
//                        }
//                    }
                } else {
                }
            }

            // Double check if the weather file name is following the old standard
            if (!daily.isEmpty() && wst_id.length() == 4) {
                String firstDay = getFirstDate(daily);
                if (firstDay != null && firstDay.length() > 3) {
                    int year = 80;
                    try {
                        year = Integer.parseInt(firstDay.substring(2, 4));
                    } catch (NumberFormatException e) {
                    }
                    if (!fileName.startsWith(wst_id + String.format("%02d", year))) {
                        wst_id = fileName;
                        clim_id = fileName.substring(4);
                    }
                }
            }

            if (!wths.containsKey(wst_id)) {
                wth.update("wst_name", wst_id);
                wth.update("wst_id", wst_id);
                wth.update("clim_id", clim_id);
                wth.update("wst_source", "DSSAT");
                wths.put(wst_id, wth);
            } else {
                try {
                    long stamp = System.currentTimeMillis();
                    addDaily(wths.get(wst_id), daily);
                    System.out.println("Cost " + ((System.currentTimeMillis() - stamp) / 1000.0) + "s");
                } catch (IOException e) {
                    LOG.warn(Functions.getStackTrace(e));
                }
                wth = null;
            }
        }

        brW.close();

        return new ArrayList(wths.values());
    }

    /**
     * Set reading flgs for title lines (marked with *)
     *
     * @param line the string of reading line
     */
    @Override
    protected void setTitleFlgs(String line) {
        flg[0] = "weather";
        flg[1] = "";
        flg[2] = "data";
    }

    /**
     * Add new daily data into array with ascending order
     *
     * @param wth original array
     * @param newDaily new data for insert
     */
    private void addDaily(AceWeather wth, AceRecordCollection newDaily) throws IOException {
        AceRecordCollection curDaily = wth.getDailyWeather();
        if (curDaily.isEmpty()) {
            curDaily.addAll(newDaily);
        } else if (!newDaily.isEmpty()) {
            int newDay;
            int curDay;
            try {
                newDay = Integer.parseInt(getFirstDate(newDaily));
            } catch (NumberFormatException e) {
                curDaily.addAll(newDaily);
                return;
            }
            ArrayList<AceRecord> tmp = new ArrayList();
            for (Iterator<AceRecord> it = curDaily.iterator(); it.hasNext();) {
                try {
                    curDay = Integer.parseInt(it.next().getValueOr("w_date", ""));
                } catch (IOException e) {
                    continue;
                }
                if (curDay > newDay) {
                    tmp.addAll(newDaily);
                    while (it.hasNext()) {
                        tmp.add(it.next());
                        it.remove();
                    }
                    break;
                }
            }
            curDaily.addAll(tmp);
        }
    }

    private String getFirstDate(AceRecordCollection daily) {
        Iterator<AceRecord> it = daily.iterator();
        try {
            return it.next().getValueOr("w_date", "");
        } catch (IOException e) {
            LOG.warn(Functions.getStackTrace(e));
            return "";
        }
    }
}
