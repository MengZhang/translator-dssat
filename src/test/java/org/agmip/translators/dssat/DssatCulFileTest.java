package org.agmip.translators.dssat;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import org.agmip.ace.io.AceParser;
import org.agmip.util.JSONAdapter;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
/**
 *
 * @author Meng Zhang
 */
public class DssatCulFileTest {

    DssatCulFileOutput obOutput;
    DssatControllerInput obInput;
//    DssatCulFileInput obInput;
    URL resource;

    @Before
    public void setUp() throws Exception {
        obOutput = new DssatCulFileOutput();
        obInput = new DssatControllerInput();
        resource = this.getClass().getResource("/APAN9304_PNX.zip");

    }

    @Test
    public void test() throws IOException, Exception {

        HashMap result;

        result = obInput.readFile(resource.getPath());
//        System.out.println(JSONAdapter.toJSON(result));
//        File f = new File("outputCul.txt");
//        BufferedOutputStream bo = new BufferedOutputStream(new FileOutputStream(f));
//        bo.write(JSONAdapter.toJSON(result).getBytes());
//        bo.close();
//        f.delete();

        obOutput.write(new File("output"), AceParser.parse(JSONAdapter.toJSON(result)));
        File file = obOutput.getOutputFile();
        if (file != null) {
            assertTrue(file.exists());
            assertEquals("APAN9304_PNX.CUL", file.getName());
            assertTrue(file.delete());
        } else {
            assertTrue(file != null);
        }
    }
}
