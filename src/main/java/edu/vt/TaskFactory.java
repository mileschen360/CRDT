package edu.vt;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;

import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class TaskFactory {
    final List<HazelcastInstance> instances;
    final static Random random = new Random(System.currentTimeMillis());
    final String GCOUNTER_GR = "GCounters";
    final String GSet_GR = "GSets";
    final HashMap<String, String> TESTED_GR_NAME;
    final int crdtId = 0;

    public TaskFactory(List<HazelcastInstance> instances){
        this.instances = instances;
        TESTED_GR_NAME = new HashMap<String, String>();
        TESTED_GR_NAME.put("Counter", "GCounters");
        TESTED_GR_NAME.put("Set", "GSets");
    }


    private HazelcastInstance getInstance(int iGateway){
        if (iGateway<0){
            iGateway = random.nextInt(instances.size());
        }
        HazelcastInstance instance = instances.get(iGateway);
        return instance;
    }

    private IMap<Integer, Crdt> getCrdts(String tested, int iGateway){
        IMap<Integer, Crdt> crdts = getInstance(iGateway).getMap(TESTED_GR_NAME.get(tested));
        return crdts;
    }

    private void base_operation(Crdt crdt, String tested, String client_id){
        if (tested.equals("Counter")){
            GCounter gCounter = (GCounter) crdt; // only one particular counter
            if (gCounter == null) {
                gCounter = new GCounter(client_id);
            } else {
                gCounter.client_id = client_id;
            }
            gCounter.inc();
        }else if (tested.equals("Set")){
            GSet gSet = (GSet) crdt; // load the gSet
            if (gSet == null) {
                gSet = new GSet(client_id);
            } else {
                gSet.client_id = client_id;
            }
            gSet.add(random.nextInt(5));
        }
    }


    private void operation(IMap<Integer, Crdt> crdts, String tested, String syncMethod, String client_id, TransactionContext context){

        if (syncMethod.equals("CRDT") || syncMethod.equals("Free")) {

            Crdt crdt = crdts.get(crdtId);
            base_operation(crdt,tested,client_id);
            crdts.put(crdtId, crdt);

        }else if (syncMethod.equals("CRDTasync")){

            Crdt crdt = crdts.get(crdtId);
            base_operation(crdt,tested,client_id);
            crdts.putAsync(crdtId, crdt);

        }else if (syncMethod.equals("Lock")){
            crdts.lock(crdtId);
            try {
                Crdt crdt = crdts.get(crdtId);
                base_operation(crdt,tested,client_id);
                crdts.putAsync(crdtId, crdt);
            }finally {
                crdts.unlock(crdtId);
            }

        }else if (syncMethod.equals("Transac2P")){
            context.beginTransaction();
            try {
                Crdt crdt = crdts.get(crdtId);
                base_operation(crdt, tested, client_id);
                crdts.put(crdtId, crdt);
                context.commitTransaction();
            } catch (Exception e){
                context.rollbackTransaction();
            }

        }else if (syncMethod.equals("TransacLocal")) { // special for local transaction
            if (tested.equals("Counter")) {
                context.beginTransaction();
                try {
                    GCounter gCounter = (GCounter) crdts.get(crdtId);
                    if (gCounter == null) {
                        gCounter = new GCounter(client_id);
                    } else {
                        gCounter.client_id = client_id;
                    }
                    gCounter.inc();
                    crdts.put(crdtId, gCounter);
                    context.commitTransaction();
                } catch (Exception e){
                    context.rollbackTransaction();
                }
            }else if (tested.equals("Set")){
                context.beginTransaction();
                try {
                    GSet gSet = (GSet) crdts.get(crdtId);
                    if (gSet == null) {
                        gSet = new GSet(client_id);
                    } else {
                        gSet.client_id = client_id;
                    }
                    gSet.add(random.nextInt(5));
                    crdts.put(crdtId, gSet);
                    context.commitTransaction();
                } catch (Exception e){
                    context.rollbackTransaction();
                }
            }
        }
    }

    public Runnable createTask(final int iGateway, final String tested, final String syncMethod, final int repeatCount) {
        return new Runnable() {
            @Override
            public void run() {
                HazelcastInstance instance = getInstance(iGateway);
                IMap<Integer, Crdt> crdts = instance.getMap(TESTED_GR_NAME.get(tested));
                String client_id = instance.getLocalEndpoint().getUuid();

                TransactionContext context = null;
                if (syncMethod.equals("Transac2P")) {
                    TransactionOptions options = new TransactionOptions().setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);
                    context = instance.newTransactionContext(options);
                }else if (syncMethod.equals("TransacLocal")){
                    TransactionOptions options = new TransactionOptions().setTransactionType(TransactionOptions.TransactionType.LOCAL);
                    context = instance.newTransactionContext(options);
                }

                for (int i = 0; i < repeatCount; ++i) {
                    operation(crdts, tested, syncMethod, client_id, context);
                }
            }
        };
    }
}
