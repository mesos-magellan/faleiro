package org.magellan.faleiro;

import org.apache.mesos.Protos;
import org.junit.Test;

import java.util.BitSet;

import static org.junit.Assert.assertTrue;

public class BitsTest {
    Bits testBeginning;

    @org.junit.Before
    public void setUp() throws Exception {
        testBeginning = new Bits();
    }

    @org.junit.After
    public void tearDown() throws Exception {
    }

    @Test
    public void testBitsToLong() throws Exception {
        BitSet testBitSet = new BitSet();
        testBitSet.set(0);
        testBitSet.set(3);
        testBitSet.set(5);
        testBitSet.set(9);
        testBitSet.set(11);
        testBitSet.set(14);

        long result;
        result = Bits.convert(testBitSet);
        assertTrue(result == 18985);
    }

    @Test
    public void testLongToBits() throws Exception {
        BitSet testConvertedBitSet;
        BitSet testConstructedBitSet = new BitSet();
        testConstructedBitSet.set(0);
        testConstructedBitSet.set(2);
        testConstructedBitSet.set(3);
        testConstructedBitSet.set(5);
        testConstructedBitSet.set(7);
        testConstructedBitSet.set(10);
        long input = 1197;
        testConvertedBitSet = Bits.convert(input);
        assertTrue(testConvertedBitSet.equals(testConstructedBitSet));
    }
}
