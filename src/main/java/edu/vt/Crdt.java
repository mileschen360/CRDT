package edu.vt;

import java.io.Serializable;

public abstract class Crdt implements Serializable{
    public Long version;
    public Object payload;

    public Crdt(){
        this.version = 0L;
        payload = null;
    }

    public abstract Object value();
    public abstract Crdt merge(Crdt another);
}