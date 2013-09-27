package org.agmip.translators.dssat;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import org.agmip.ace.AceDataset;
import org.agmip.ace.io.AceParser;
import org.agmip.util.JSONAdapter;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
/**
 *
 * @author Meng Zhang
 */
public class DssatControllerTest {

    DssatControllerOutput obDssatControllerOutput;
    DssatControllerInput obDssatControllerInput;
    URL resource;
    String fileName =
//                        "UFGA8401_CPX.ZIP";
//                        "GHWA0401_MZX.ZIP";
//                        "UFGA8201_MZX.ZIP";
//                        "UFGA8202_MZX.ZIP";
//                        "UFGA8201_MZX_dummy.ZIP";
                        "CISR9704_VBX.ZIP";
//                        "SWData.zip";
//                        "AGMIP_DSSAT_1359154224136.zip";
//                        "HSC_wth_bak.zip";

    @Before
    public void setUp() throws Exception {
        obDssatControllerOutput = new DssatControllerOutput();
        obDssatControllerInput = new DssatControllerInput();
        resource = this.getClass().getResource("/" + fileName);
    }

    @Test
//    @Ignore
    public void test() throws IOException, Exception {
        HashMap result;
        AceDataset ace;
        Calendar cal;
        String outPath;
        File outDir;
        List<File> files;
        result = obDssatControllerInput.readFile(resource.getPath());
        ace = AceParser.parse(JSONAdapter.toJSON(result));

        BufferedOutputStream bo;
        File f = new File("output\\" + fileName.replaceAll("[Xx]*\\.\\w+$", ".json"));
        bo = new BufferedOutputStream(new FileOutputStream(f));

        // Output json for reading
        bo.write(JSONAdapter.toJSON(result).getBytes());
        bo.close();
        f.delete();

        cal = Calendar.getInstance();
        outPath = "output\\AGMIP_DSSAT_" + cal.getTimeInMillis();
        outDir = new File(outPath);
        obDssatControllerOutput = new DssatControllerOutput();
        files = obDssatControllerOutput.write(outDir, ace);
        for (File file : files) {
            System.out.println("Generated: " + file.getName());
            file.delete();
        }
        outDir.delete();

        String jsonStr;
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("Machakos_1Exp-1Yr.json");
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        jsonStr = br.readLine();
        cal = Calendar.getInstance();
        outPath = "output\\AGMIP_DSSAT_" + cal.getTimeInMillis();
        outDir = new File(outPath);
        obDssatControllerOutput = new DssatControllerOutput();
        files = obDssatControllerOutput.write(outDir, AceParser.parse(jsonStr));
        for (File file : files) {
            System.out.println("Generated: " + file.getName());
            file.delete();
        }
        outDir.delete();
    }
}
