package edu.vt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PNCounter extends Crdt implements Counter{
    public String client_id;
    public Map<String, Integer> payload;

    public PNCounter(){
        this.version = 0L;
        payload = new HashMap<String, Integer>();
        // use concurrentHashmap if allow two identical clients
        // running simultaneously.
    }

    public PNCounter(String client_id){
        this.version = 0L;
        this.client_id = client_id;
        payload = new HashMap<String, Integer>();
    }

    public int width(){
        return payload.size();
    }


    boolean less_at(String id, PNCounter another){
        Integer c_this = this.payload.get(id);
        Integer c_another = another.payload.get(id);
        if (c_this == null){
            return true;
        }else if (c_another == null){
            return false;
        }else{
            return c_this < c_another;
        }
    }

    @Override
    public void inc(){
        //System.out.println(client_id);
        Integer c = payload.get(client_id);
        if (c==null){
            c = 0;
            //System.out.println("c=0");
        }
        payload.put(client_id, c + 1);
    }

    @Override
    public void decrease() throws Exception {
        throw new FeatureAbsentException();
    }

    @Override
    public Object value(){
        int sum = 0;
        //System.out.println("payload n_members: "+payload.size());
        for(int c : payload.values()){
            sum += c;
        }
        return sum;
    }

    @Override
    public Crdt merge(Crdt another) {
        PNCounter merged = new PNCounter();
        Set<String> client_ids = new HashSet<String>(this.payload.keySet());
        client_ids.addAll(((PNCounter) another).payload.keySet());
        for (String id : client_ids){
            if (this.less_at(id, (PNCounter) another)){
                merged.payload.put(id, ((PNCounter) another).payload.get(id));
            }else{
                merged.payload.put(id, this.payload.get(id));
            }
        }
        merged.version = Math.max(this.version, another.version);
        return merged;

    }

}
