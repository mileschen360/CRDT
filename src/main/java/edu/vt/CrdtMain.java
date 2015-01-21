package edu.vt;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.map.MapInterceptor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class CrdtMain {
    static int nClusterNodes = 4;
    static int nNodesInTest = 4;
    static int nTestThreads = 4;
    static String tested = "Counter";
    static String syncMethod = "CRDT";
    static String fp_result = "test_result.txt";
    //static String testType = "performance";
    static String testType = "correctness";

    final List<HazelcastInstance> instances;
    final int crdtId = 0;
    final String GCOUNTER_GR = "GCounters";
    final String GSet_GR = "GSets";
    final static Random random = new Random(System.currentTimeMillis());
    MyMergeInterceptor interceptor;
    String interceptor_reg_id;
    BufferedWriter fout;

    public CrdtMain() throws InterruptedException, IOException {
        FileWriter fstream = new FileWriter(fp_result);
        fout = new BufferedWriter(fstream);
        interceptor = new MyMergeInterceptor();

        System.out.println("**** Start setting up cluster");
        instances = new ArrayList<HazelcastInstance>();
        //ClientConfig clientConfig = new ClientConfig().addAddress("127.0.0.1");
        Config cfg = new Config();
        //cfg.setProperty("hazelcast.logging.type", "none");
        for (int i = 0; i < nClusterNodes; i++) {// create 4 client
            HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(cfg);
            instances.add(hazelcastInstance);
        }

        System.out.println("\n\n\n" +
                "*****************************\n" +
                "Cluster set up. Starting running our application on cluster!\n");
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        /// simple command line option parser
        for(int i=0; i<args.length; ++i){
            if (args[i].equals("--help")){
                System.out.println("Usage example:");
                System.out.println("java edu.vt.CrdtMain --testType correctness --tested Set --syncMethod CRDT --nTestThreads 10");
                System.exit(0);
            }

            if (args[i].equals("--nTestThreads")){
                nTestThreads = Integer.parseInt(args[i+1]);
            }

            if (args[i].equals("--nClusterNodes")){
                nClusterNodes = Integer.parseInt(args[i+1]);
            }

            if (args[i].equals("--nNodesInTest")){
                nNodesInTest = Integer.parseInt(args[i+1]);
            }

            if (args[i].equals("--testType")){
                testType = args[i+1];
            }

            if (args[i].equals("--tested")){
                tested = args[i+1];
            }

            if (args[i].equals("--syncMethod")){
                syncMethod = args[i+1];
            }

            if (args[i].equals("--output")){
                fp_result = args[i+1];
            }
        }

        CrdtMain crdtMain = new CrdtMain();

        crdtMain.prepare_test();
        if (testType.equals("performance")) {
            crdtMain.test_performance();
        }else if (testType.equals("correctness")){
            crdtMain.test_correctness();
        }
    }


    private void prepare_test(){
        if (tested.equals("Counter")) {
            /// initialize a gcounter
            IMap<Integer, GCounter> gCounters = getInstance().getMap(GCOUNTER_GR);
            if (syncMethod.equals("CRDT")) {
                gCounters.addInterceptor(interceptor);
                System.out.println("CRDT support layer set up");
            }

            int gCounterId = 0; // like empleeId
            gCounters.put(gCounterId, new GCounter()); // gCounters is an abstract table, don't cache entire table to local
        }else if (tested.equals("Set")) {
            /// initialize a gset
            IMap<Integer, GSet> gSets = getInstance().getMap(GSet_GR);
            if (syncMethod.equals("CRDT")) {
                interceptor_reg_id = gSets.addInterceptor(interceptor);
            }

            gSets.put(crdtId, new GSet());
        }
    }


    private void test_performance() throws InterruptedException, IOException {
        PerformanceTester tester = new PerformanceTester(instances.get(0), nTestThreads, fout);
        TaskFactory taskFactory = new TaskFactory(instances);

        int dataUpdateCount = 1;
        int iGatewayNode = -1; // -1 means random
        Runnable task = taskFactory.createTask(iGatewayNode, tested, syncMethod, dataUpdateCount);

        tester.start(task);
        Thread.sleep(10000);
        tester.stop();
    }

    private void test_correctness() throws InterruptedException {
        CorrectnessTester tester = new CorrectnessTester(nTestThreads);
        TaskFactory taskFactory = new TaskFactory(instances);

        int nTasks = nClusterNodes;
        int dataUpdateCount_perTask = 17;
        //int iGatewayNode = -1; // -1 means random

        Runnable tasks[] = new Runnable[nTasks];
        for(int i=0; i<nTasks; i++){
            tasks[i] = taskFactory.createTask(i, tested, syncMethod, dataUpdateCount_perTask);
         }

        tester.start(tasks);

        tester.awaitTermination();

        /// check correctness
        IMap<Integer,Crdt> crdts;
        crdts = getInstance().getMap(GCOUNTER_GR);
        Crdt crdt = crdts.get(crdtId);
        Integer sum = (Integer) crdt.value();
        System.out.println("value="+sum);
        //System.out.println(gCounter.width());
        if (syncMethod.equals("CRDT")) {
            if (nTasks * dataUpdateCount_perTask == sum)
                System.out.println("All Tests Passed");
            else
                System.out.println("Test failed");
        }
        //Hazelcast.shutdownAll();
        System.exit(0);

    }


    /// randomly select a node to avoid contention
    public HazelcastInstance getInstance() {
        return instances.get(random.nextInt(instances.size()));
    }


    static class MyMergeInterceptor implements MapInterceptor{

        @Override
        public Object interceptGet(Object o) {
            return null;
        }

        @Override
        public void afterGet(Object o) {

        }

        @Override
        public Object interceptPut(Object oldValue, Object value) {
            Crdt crdt = (Crdt) value;
            Crdt modified;
            if (oldValue==null){
                modified = crdt;
            } else{
                Crdt old_crdt = (Crdt) oldValue;
                if (old_crdt.version.equals(crdt.version)) {
                    modified = crdt;
                }else{
                    //System.out.println("Conflict!!!");
                    modified = old_crdt.merge(crdt);
                }
            }

            modified.version++;
            return modified;
        }

        @Override
        public void afterPut(Object o) {

        }

        @Override
        public Object interceptRemove(Object o) {
            return null;
        }

        @Override
        public void afterRemove(Object o) {

        }
    }
}



