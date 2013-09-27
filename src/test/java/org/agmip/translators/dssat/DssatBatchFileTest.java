package org.agmip.translators.dssat;

import java.io.File;
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
public class DssatBatchFileTest {

    DssatControllerInput obDssatControllerInput;
    DssatBatchFileOutput obDssatBatchFileOutput;
    URL resource;

    @Before
    public void setUp() throws Exception {
        obDssatControllerInput = new DssatControllerInput();
        obDssatBatchFileOutput = new DssatBatchFileOutput();
        resource = this.getClass().getResource("/UFGA8201_MZX.zip");
    }

    @Test
    public void test() throws IOException, Exception {
        HashMap result;

        result = obDssatControllerInput.readFile(resource.getPath());

        obDssatBatchFileOutput.write(new File("output"), AceParser.parse(JSONAdapter.toJSON(result)));
        File file = obDssatBatchFileOutput.getOutputFile();
        if (file != null) {
            assertTrue(file.exists());
            assertEquals("DSSBatch.v45", file.getName());
            assertTrue(file.delete());
        } else {
            assertTrue(file != null);
        }
    }
}
