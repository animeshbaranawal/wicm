package in.dreamlab.wicm.graph.mutations;

import in.dreamlab.wicm.conf.WICMConstants;
import in.dreamlab.wicm.graph.computation.GraphiteDebugWindowWorkerContext;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import java.util.concurrent.Semaphore;

public class WICMMutationsWorkerContext extends GraphiteDebugWindowWorkerContext implements WICMConstants {
    private static final Logger LOG = Logger.getLogger(WICMMutationsWorkerContext.class);

    private static final String Mutation = "isMutationSuperstep";
    private static final String WindowNum = "windowNumber";

    public Semaphore fileLock;
    public String mutationFileName;

    @Override
    public void preSuperstep() {
        //if mutation superstep
        if(((BooleanWritable) getAggregatedValue(Mutation)).get()) {
            //then record windownumber and write to logs
            fileLock = new Semaphore(1);
            int windowNumber = ((IntWritable) getAggregatedValue(WindowNum)).get();
            mutationFileName = MUTATION_PATH.get(getConf()) + "-" + windowNumber + "-" + getMyWorkerIndex();
            LOG.info("WindowNumber:"+windowNumber+", Filename:"+mutationFileName);
        }
        //else if first superstep
        else if(((BooleanWritable) getAggregatedValue(Init)).get()) {
            //initialize logging
            try {
                String filenamePath = RESOLVER_SUBDIR.get(getConf())+
                        "/dump-"+getSuperstep()+"-"+getMyWorkerIndex();
                resolverOutput.initialise(filenamePath, getConf());
            } catch (Exception e) {
                LOG.info("Caught exception: "+ e);
            }
        }
        //execute superstep of superior
        super.preSuperstep();
    }

    @Override
    public void postSuperstep() {
        if(((BooleanWritable) getAggregatedValue(Init)).get()) {
            try {
                resolverOutput.close();
            } catch (Exception e) {
                LOG.info("Caught exception: "+ e);
            }
            System.gc();
        }
        //execute post superstep of parent class
        super.postSuperstep();
    }
}
