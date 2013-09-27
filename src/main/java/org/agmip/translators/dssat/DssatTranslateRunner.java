package org.agmip.translators.dssat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.agmip.ace.AceDataset;
import org.agmip.ace.AceExperiment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translation runner for multiple thread mode
 *
 * @author Meng Zhang
 */
public class DssatTranslateRunner implements Callable<List<File>> {

    private DssatCommonOutput translator;
    private AceDataset ace;
    private List<AceExperiment> GourpedExps;
    private File outDir;
    private static Logger LOG = LoggerFactory.getLogger(DssatTranslateRunner.class);

    public DssatTranslateRunner(DssatCommonOutput translator, AceDataset data, File outputDirectory) {
        this.translator = translator;
        this.ace = data;
        this.outDir = outputDirectory;
    }

    public DssatTranslateRunner(DssatCommonOutput translator, List<AceExperiment> dataArr, File outputDirectory) {
        this.translator = translator;
        this.GourpedExps = dataArr;
        this.outDir = outputDirectory;
    }

    @Override
    public List<File> call() throws Exception {
        LOG.debug("Starting new thread!");
        List<File> ret = new ArrayList();
        try {
            if (translator instanceof DssatXATFileOutputI) {
                ((DssatXATFileOutputI) translator).writeFile(DssatCommonOutput.revisePath(outDir), GourpedExps);
                if (translator.getOutputFile() != null) {
                    ret.add(translator.getOutputFile());
                }
            } else {
                ret.addAll(translator.write(outDir, ace));
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }

        if (ret.isEmpty()) {
            LOG.debug("Job canceled!");
            throw new NoOutputFileException();
        } else {
            LOG.debug("Job done for {}", ret);
            return ret;
        }
    }

    class NoOutputFileException extends RuntimeException {
    }
}
