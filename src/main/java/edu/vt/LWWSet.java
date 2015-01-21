package edu.vt;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class LWWSet extends Crdt implements Set{
    public HashSet<Integer> payload;
    public String client_id;

    public LWWSet(){
        super();
        payload = new HashSet<Integer>();
    }

    public LWWSet(String client_id){
        this();
        this.client_id = client_id;
    }

    @Override
    public Object value(){
        return payload;
    }

    @Override
    public Crdt merge(Crdt another) {
        LWWSet merged = new LWWSet();
        merged.payload.addAll(this.payload);
        merged.payload.addAll(((LWWSet)another).payload);
        return merged;
    }

    public void add(Integer e){
        payload.add(e);
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public Iterator iterator() {
        return null;
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public boolean add(Object o) {
        return false;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean addAll(Collection c) {
        return false;
    }

    @Override
    public void clear() {

    }

    @Override
    public boolean removeAll(Collection c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection c) {
        return false;
    }

    @Override
    public boolean containsAll(Collection c) {
        return false;
    }

    @Override
    public Integer[] toArray(Object[] a) {
        return new Integer[0];
    }
}
