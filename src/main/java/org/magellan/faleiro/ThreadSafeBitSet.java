package org.magellan.faleiro;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * Created by abilinski on 28/03/16.
 */
public class ThreadSafeBitSet extends BitSet {
    /*
    private final Object lock = new Object();
    private final BitSet mybitset;

    public ThreadSafeBitSet(int varl){
        this.mybitset = new BitSet(varl);
    }
    public ThreadSafeBitSet(){
        this.mybitset = new BitSet();
    }

    public boolean get(int i){
        synchronized(lock){
            return mybitset.get(i);
        }
    }

    public int cardinality(){
        synchronized (lock){
            return mybitset.cardinality();
        }
    }

    public void set(int i){
        synchronized (lock){
            mybitset.set(i);
        }
    }

    public byte[] toByteArray(){
        synchronized (lock){
            return mybitset.toByteArray();
        }
    }

    public static ThreadSafeBitSet valueOf(byte[] var0) {
        return valueOf(ByteBuffer.wrap(var0));
    }
    */
}
