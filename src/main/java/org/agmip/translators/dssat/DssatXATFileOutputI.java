package org.agmip.translators.dssat;

import java.util.List;
import org.agmip.ace.AceExperiment;

/**
 *
 * @author Meng Zhang
 */
public interface DssatXATFileOutputI {
    /**
     * Output grouped experiment information
     * 
     * @param dir the output directory
     * @param exps the grouped experiment data
     */
    public void writeFile(String dir, List<AceExperiment> exps);
}
