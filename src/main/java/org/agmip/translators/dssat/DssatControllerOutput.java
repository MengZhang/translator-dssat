package org.agmip.translators.dssat;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.agmip.ace.AceBaseComponentType;
import org.agmip.ace.AceDataset;
import org.agmip.ace.AceExperiment;
import org.agmip.ace.AceWeather;
import org.agmip.translators.dssat.DssatBatchFileOutput.DssatVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Meng Zhang
 */
public class DssatControllerOutput extends DssatCommonOutput {

    private ArrayList<File> files = new ArrayList();
    private HashMap<String, Future<List<File>>> futFileList = new HashMap();
//    private HashMap<String, Future<File>> soilFiles = new HashMap();
//    private HashMap<String, Future<File>> wthFiles = new HashMap();
//    private HashMap<String, Map> soilData = new HashMap();
//    private HashMap<String, Map> wthData = new HashMap();
    private ExecutorService executor = Executors.newFixedThreadPool(64);
    private static final Logger LOG = LoggerFactory.getLogger(DssatControllerOutput.class);
//    private ArrayList<File> 
    
    public DssatControllerOutput() {
        soilHelper = new DssatSoilFileHelper();
        wthHelper = new DssatWthFileHelper();
    }

    /**
     * ALL DSSAT Data Output method
     *
     * @param outDir the directory to output the translated files.
     * @param ace the source ACE Dataset
     * @param components subcomponents to translate
     *
     * @return the list of generated files
     */
    @Override
    public List<File> write(File outDir, AceDataset ace, AceBaseComponentType... components) throws IOException {
        
        long estTime = 0;
        ace.linkDataset();

        try {
            // Calculate estimated time consume
            for (AceWeather wth : ace.getWeathers()) {
                estTime += wth.getDailyWeather().size();
            }
            Map<String, List<AceExperiment>> expGroup = groupingExpData(ace);
            for (String key : expGroup.keySet()) {
                List<AceExperiment> exps = expGroup.get(key);
                writeSingleExp(outDir, exps, new DssatXFileOutput(), key + "_X");
                writeSingleExp(outDir, exps, new DssatAFileOutput(), key + "_A");
                writeSingleExp(outDir, exps, new DssatTFileOutput(), key + "_T");
                writeSingleExp(outDir, exps, new DssatCulFileOutput(), key + "_Cul");
            }
            writeSingleExp(outDir, ace, new DssatSoilOutput(), "Soil file");
            writeSingleExp(outDir, ace, new DssatWeatherOutput(), "Weather file");
            writeSingleExp(outDir, ace, new DssatBatchFileOutput(DssatVersion.DSSAT45), "DSSBatch.v45");
            writeSingleExp(outDir, ace, new DssatBatchFileOutput(DssatVersion.DSSAT46), "DSSBatch.v46");
            writeSingleExp(outDir, ace, new DssatRunFileOutput(DssatVersion.DSSAT45), "Run45.bat");
            writeSingleExp(outDir, ace, new DssatRunFileOutput(DssatVersion.DSSAT46), "Run46.bat");
            
            if (estTime > 0) {
                estTime *= 10;
            } else {
                estTime = 10000;
            }

            executor.shutdown();
            long timer = System.currentTimeMillis();
            HashSet<String> keys = new HashSet(futFileList.keySet());
            while (!executor.isTerminated()) {
                if (System.currentTimeMillis() - timer > estTime) {
                    String[] arr = keys.toArray(new String[0]);
                    for (String key : arr) {
                        Future futFiles = futFileList.get(key);
                        if (futFiles.isCancelled() || futFiles.isDone()) {
                            keys.remove(key);
                        } else {
                            LOG.info("DSSAT translation for {} is still under processing...", key);
                        }
                    }
                } else {
                    timer = System.currentTimeMillis();
                }
            }
            executor = null;

            // Get the list of generated files
            for (String key : futFileList.keySet()) {
                Future<List<File>> futFiles = futFileList.get(key);
                try {
                    List<File> fl = futFiles.get();
                    if (fl != null) {
                        files.addAll(fl);
                    }
                } catch (InterruptedException ex) {
                    LOG.error(getStackTrace(ex));
                } catch (ExecutionException ex) {
                    if (!ex.getMessage().contains("NoOutputFileException")) {
                        LOG.error(getStackTrace(ex));
                    }
                }
            }

        } catch (FileNotFoundException e) {
            LOG.error(getStackTrace(e));
        } catch (IOException e) {
            LOG.error(getStackTrace(e));
        }
        
        return getOutputFiles();
    }

    /**
     * Write files and add file objects in the array
     *
     * @param outDir file output path
     * @param ace data holder object
     * @param output DSSAT translator object
     * @param file Generated DSSAT file identifier
     */
    private void writeSingleExp(File outDir, AceDataset ace, DssatCommonOutput output, String file) {
        futFileList.put(file, executor.submit(new DssatTranslateRunner(output, ace, outDir)));
    }

    /**
     * Write files and add file objects in the array
     *
     * @param outDir file output path
     * @param groupedExps the list of grouped experiment data
     * @param output DSSAT translator object
     * @param file Generated DSSAT file identifier
     */
    private void writeSingleExp(File outDir, List<AceExperiment> groupedExps, DssatCommonOutput output, String file) {
        futFileList.put(file, executor.submit(new DssatTranslateRunner(output, groupedExps, outDir)));
    }

    /**
     * Compress the files in one zip
     *
     * @throws FileNotFoundException
     * @throws IOException
     */
    private void createZip() throws FileNotFoundException, IOException {

        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(outputFile));
        ZipEntry entry;
        BufferedInputStream bis;
        byte[] data = new byte[1024];

        // Get output result files into output array for zip package
        long timer = System.currentTimeMillis();
        for (String key : futFileList.keySet()) {
            try {
                List<File> fl = futFileList.get(key).get();
                if (fl != null) {
                    files.addAll(fl);
                }
            } catch (InterruptedException ex) {
                LOG.error(getStackTrace(ex));
            } catch (ExecutionException ex) {
                if (!ex.getMessage().contains("NoOutputFileException")) {
                    LOG.error(getStackTrace(ex));
                }
            }
        }
        LOG.debug("Consume {} s", (System.currentTimeMillis() - timer) / 1000.0);

        LOG.info("Start zipping all the files...");

        // Check if there is file been created
        if (files == null) {
            LOG.warn("No files here for zipping");
            return;
        }

        for (File file : files) {
            if (file == null) {
                continue;
            }

            if (outputFile.getParent() != null) {
                entry = new ZipEntry(file.getPath().substring(outputFile.getParent().length() + 1));
            } else {
                entry = new ZipEntry(file.getPath());
            }
            out.putNextEntry(entry);
            bis = new BufferedInputStream(new FileInputStream(file));

            int count;
            while ((count = bis.read(data)) != -1) {
                out.write(data, 0, count);
            }
            bis.close();
            file.delete();
        }

        out.close();
        LOG.info("End zipping");
    }

    /**
     * Get all output files
     */
    public ArrayList<File> getOutputFiles() {
        return files;
    }

    /**
     * Get output zip file name by using experiment file name
     *
     * @param outputs DSSAT Output objects
     */
    private String getZipFileName(DssatCommonOutput[] outputs) {

        for (int i = 0; i < outputs.length; i++) {
            if (outputs[i] instanceof DssatXFileOutput) {
                if (outputs[i].getOutputFile() != null) {
                    return outputs[i].getOutputFile().getName().replaceAll("\\.", "_") + ".ZIP";
                } else {
                    break;
                }
            }
        }

        return "OUTPUT.ZIP";
    }

    /**
     * Get output zip file
     */
    public File getOutputZipFile() {
        return outputFile;
    }
}
