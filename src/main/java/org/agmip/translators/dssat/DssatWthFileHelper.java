package org.agmip.translators.dssat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import org.agmip.ace.AceRecord;
import org.agmip.ace.AceRecordCollection;
import org.agmip.ace.AceWeather;
import static org.agmip.translators.dssat.DssatCommonOutput.getValueOr;

/**
 *
 * @author Meng Zhang
 */
public class DssatWthFileHelper {

    private HashSet<String> names = new HashSet();
    private HashMap<String, String> hashToName = new HashMap();
    private int defInsiName = 0xAAAA;

    /**
     * Generate the weather file name for auto-generating (extend name not
     * included)
     *
     * @param wthData weather data holder
     * @return the weather file name
     */
    public String createWthFileName(AceWeather wthData) throws IOException {

        String hash = getValueOr(wthData, "wst_id", "").toString();
        if (hash.length() == 8) {
            return hash;
        }
        if (hashToName.containsKey(hash)) {
            return hashToName.get(hash);
        } else {
            String insiName = getWthInsiCodeOr(wthData);
            String yearDur = getWthYearDuration(wthData);
            String wst_id = insiName;
            if (insiName.length() == 4) {
                String clim_id = getValueOr(wthData, "clim_id", "0XXX");
                if (!clim_id.startsWith("0")) {
                    wst_id += clim_id;
                } else {
                    wst_id += yearDur;
                }
            }
            while (names.contains(wst_id)) {
                wst_id = getNextDefName() + yearDur;
            }
            names.add(wst_id);
            if (hash.equals("")) {
                hash = wst_id;
            }
            hashToName.put(hash, wst_id);
            return wst_id;
        }
    }

    /**
     * Get the 4-bit institute code for weather file name, if not available from
     * data holder, then use default code
     *
     * @param wthData Weather data holder
     * @return
     */
    private String getWthInsiCodeOr(AceWeather wthData) throws IOException {
        String insiName = getWthInsiCode(wthData);
        if (insiName.equals("")) {
            return getNextDefName();
        } else {
            return insiName;
        }
    }

    /**
     * Generate the institute code
     *
     * @return
     */
    private String getNextDefName() {
        return Integer.toHexString(defInsiName++).toUpperCase();
    }

    /**
     * Get the 4-bit institute code for weather file nam
     *
     * @param wthData weather data holder
     * @return the 4-bit institute code
     */
    public static String getWthInsiCode(AceWeather wthData) throws IOException {
        String wst_name = getValueOr(wthData, "wst_name", "");
        if (wst_name.matches("(\\w{4})|(\\w{8})")) {
            return wst_name;
        }

        String wst_id = getValueOr(wthData, "wst_id", "");
        if (wst_id.matches("(\\w{4})|(\\w{8})")) {
            return wst_id;
        }

        wst_id = getValueOr(wthData, "dssat_insi", "");
        if (wst_id.matches("(\\w{4})|(\\w{8})")) {
            return wst_id;
        }

        return "";
    }

    /**
     * Get the last 2-bit year number and 2-bit of the duration for weather file
     * name
     *
     * @param wthData weather data holder
     * @return the 4-bit number for year and duration
     */
    public static String getWthYearDuration(AceWeather wthData) throws IOException {
        String yearDur = "";
        AceRecordCollection dailys = ((AceWeather) wthData).getDailyWeather();
        Iterator<AceRecord> it = dailys.iterator();
        if (it.hasNext()) {
            AceRecord daily = it.next();
            String startYear = daily.getValueOr("w_date", "    ").substring(2, 4).trim();
            while (it.hasNext()) {
                daily = it.next();
            }
            String endYear = daily.getValueOr("w_date", "    ").substring(2, 4).trim();
            yearDur = getWthYearDuration(yearDur, startYear, endYear);
        }

        return yearDur;
    }

    private static String getWthYearDuration(String yearDur, String startYear, String endYear) {

        // If not available, do not show year and duration in the file name
        if (!startYear.equals("") && !endYear.equals("")) {
            yearDur += startYear;
            try {
                int iStartYear = Integer.parseInt(startYear);
                int iEndYear = Integer.parseInt(endYear);
                iStartYear += iStartYear <= 15 ? 2000 : 1900; // P.S. 2015 is the cross year for the current version
                iEndYear += iEndYear <= 15 ? 2000 : 1900; // P.S. 2015 is the cross year for the current version
                int duration = iEndYear - iStartYear + 1;
                // P.S. Currently the system only support the maximum of 99 years for duration
                duration = duration > 99 ? 99 : duration;
                yearDur += String.format("%02d", duration);
            } catch (Exception e) {
                yearDur += "01";    // Default duration uses 01 (minimum value)
            }
        }

        return yearDur;
    }
}
