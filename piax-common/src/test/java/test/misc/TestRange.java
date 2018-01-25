package test.misc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.piax.common.subspace.Range;

public class TestRange {

    @Test
    public void test() {
        {
            Range<Integer> r1 = new Range<>(0, false, 10, false);
            Range<Integer> r2 = new Range<>(0, false, 10, true);
            Range<Integer> r3 = new Range<>(0, true, 10, false);
            Range<Integer> r4 = new Range<>(0, true, 10, true);
    
            assertTrue(r4.contains(r1));
            assertTrue(r4.contains(r2));
            assertTrue(r4.contains(r3));
            assertTrue(r4.contains(r4));
    
            assertTrue(r3.contains(r1));
            assertFalse(r3.contains(r2));
            assertTrue(r3.contains(r3));
            assertFalse(r3.contains(r4));
        }
    }
    
    @Test
    public void test1() {
        Range<Integer> r5_10 = new Range<>(5, true, 10, false);
        // subtrahend min = 0, vary max
        {
            Range<Integer> r0_1 = new Range<>(0, true, 1, false);
            assertFalse(r5_10.contains(r0_1));
            assertFalse(r0_1.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r0_1, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(5, true, 10, false)));
            assertTrue(removed.size() == 0);
        }
        {
            Range<Integer> r0_5 = new Range<>(0, true, 5, false);
            assertFalse(r5_10.contains(r0_5));
            assertFalse(r0_5.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r0_5, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(5, true, 10, false)));
            assertTrue(removed.size() == 0);
        }
        {
            Range<Integer> r0_8 = new Range<>(0, true, 8, false);
            assertFalse(r5_10.contains(r0_8));
            assertFalse(r0_8.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r0_8, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(8, true, 10, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(5, true, 8, false)));
        }
        {
            Range<Integer> r0_10 = new Range<>(0, true, 10, false);
            assertFalse(r5_10.contains(r0_10));
            assertTrue(r0_10.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r0_10, removed);
            assertTrue(retain.isEmpty());
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(5, true, 10, false)));
        }
        {
            Range<Integer> r0_12 = new Range<>(0, true, 12, false);
            assertFalse(r5_10.contains(r0_12));
            assertTrue(r0_12.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r0_12, removed);
            assertTrue(retain.isEmpty());
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(5, true, 10, false)));
        }
        {
            Range<Integer> r0_minus1 = new Range<>(true, 0, true, -1, false);
            assertFalse(r5_10.contains(r0_minus1));
            assertTrue(r0_minus1.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r0_minus1, removed);
            assertTrue(retain.isEmpty());
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(5, true, 10, false)));
        }

        // subtrahend min = 5, vary max
        {
            Range<Integer> r5_6 = new Range<>(5, true, 6, false);
            assertTrue(r5_10.contains(r5_6));
            assertFalse(r5_6.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r5_6, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(6, true, 10, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(5, true, 6, false)));
        }
        {
            Range<Integer> r5_10c = new Range<>(5, true, 10, false);
            assertTrue(r5_10.contains(r5_10c));
            assertTrue(r5_10c.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r5_10c, removed);
            assertTrue(retain.isEmpty());
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(5, true, 10, false)));
        }
        {
            Range<Integer> r5_12 = new Range<>(5, true, 12, false);
            assertFalse(r5_10.contains(r5_12));
            assertTrue(r5_12.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r5_12, removed);
            assertTrue(retain.isEmpty());
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(5, true, 10, false)));
        }
        {
            Range<Integer> r5_0 = new Range<>(true, 5, true, 0, false);
            assertFalse(r5_10.contains(r5_0));
            assertTrue(r5_0.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r5_0, removed);
            assertTrue(retain.isEmpty());
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(5, true, 10, false)));
        }
        // subtrahend min = 8, vary max
        {
            Range<Integer> r8_9 = new Range<>(8, true, 9, false);
            assertTrue(r5_10.contains(r8_9));
            assertFalse(r8_9.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r8_9, removed);
            assertTrue(retain.size() == 2);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(5, true, 8, false)));
            assertTrue(retain.get(1).isSameRange(new Range<Integer>(9, true, 10, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(8, true, 9, false)));
        }
        {
            Range<Integer> r8_10 = new Range<>(8, true, 10, false);
            assertTrue(r5_10.contains(r8_10));
            assertFalse(r8_10.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r8_10, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(5, true, 8, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(8, true, 10, false)));
        }
        {
            Range<Integer> r8_12 = new Range<>(8, true, 12, false);
            assertFalse(r5_10.contains(r8_12));
            assertFalse(r8_12.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r8_12, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(5, true, 8, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(8, true, 10, false)));
        }
        {
            Range<Integer> r8_0 = new Range<>(true, 8, true, 0, false);
            assertFalse(r5_10.contains(r8_0));
            assertFalse(r8_0.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r8_0, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(5, true, 8, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(8, true, 10, false)));
        }

        // subtrahend min = 10, vary max
        {
            Range<Integer> r10_10 = new Range<>(10, true, 10, true);
            assertFalse(r5_10.contains(r10_10));
            assertFalse(r10_10.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r10_10, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(5, true, 10, false)));
            assertTrue(removed.isEmpty());
        }
        {
            Range<Integer> r10_0 = new Range<>(true, 10, true, 0, false);
            assertFalse(r5_10.contains(r10_0));
            assertFalse(r10_0.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r10_0, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(5, true, 10, false)));
            assertTrue(removed.isEmpty());
        }
        {
            Range<Integer> r10_5 = new Range<>(true, 10, true, 5, false);
            assertFalse(r5_10.contains(r10_5));
            assertFalse(r10_5.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r10_5, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(5, true, 10, false)));
            assertTrue(removed.isEmpty());
        }
        {
            Range<Integer> r10_8 = new Range<>(true, 10, true, 8, false);
            assertFalse(r5_10.contains(r10_8));
            assertFalse(r10_8.contains(r5_10));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_10.retain(r10_8, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(8, true, 10, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(5, true, 8, false)));
        }
    }
    
    @Test
    public void test2() {
        Range<Integer> r10_5 = new Range<>(true, 10, true, 5, false);
        // subtrahend min = 0, vary max
        {
            Range<Integer> r0_1 = new Range<>(0, true, 1, false);
            assertTrue(r10_5.contains(r0_1));
            assertFalse(r0_1.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r0_1, removed);
            assertTrue(retain.size() == 2);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, 10, true, 0, false)));
            assertTrue(retain.get(1).isSameRange(new Range<Integer>(1, true, 5, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(0, true, 1, false)));
        }
        {
            Range<Integer> r0_5 = new Range<>(0, true, 5, false);
            assertTrue(r10_5.contains(r0_5));
            assertFalse(r0_5.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r0_5, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, 10, true, 0, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(0, true, 5, false)));
        }
        {
            Range<Integer> r0_8 = new Range<>(0, true, 8, false);
            assertFalse(r10_5.contains(r0_8));
            assertFalse(r0_8.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r0_8, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, 10, true, 0, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(0, true, 5, false)));
        }
        {
            Range<Integer> r0_10 = new Range<>(0, true, 10, false);
            assertFalse(r10_5.contains(r0_10));
            assertFalse(r0_10.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r0_10, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, 10, true, 0, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(0, true, 5, false)));
        }
        {
            Range<Integer> r0_12 = new Range<>(0, true, 12, false);
            assertFalse(r10_5.contains(r0_12));
            assertFalse(r0_12.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r0_12, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, 12, true, 0, false)));
            assertTrue(removed.size() == 2);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(10, true, 12, false)));
            assertTrue(removed.get(1).isSameRange(new Range<Integer>(0, true, 5, false)));
        }
        {
            Range<Integer> r0_minus1 = new Range<>(true, 0, true, -1, false);
            assertFalse(r10_5.contains(r0_minus1));
            assertFalse(r0_minus1.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r0_minus1, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, -1, true, 0, false)));
            assertTrue(removed.size() == 2);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(true, 10, true, -1, false)));
            assertTrue(removed.get(1).isSameRange(new Range<Integer>(true, 0, true, 5, false)));
        }

        // subtrahend min = 5, vary max
        {
            Range<Integer> r5_6 = new Range<>(5, true, 6, false);
            assertFalse(r10_5.contains(r5_6));
            assertFalse(r5_6.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r5_6, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, 10, true, 5, false)));
            assertTrue(removed.isEmpty());
        }
        {
            Range<Integer> r5_10 = new Range<>(5, true, 10, false);
            assertFalse(r10_5.contains(r5_10));
            assertFalse(r5_10.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r5_10, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, 10, true, 5, false)));
            assertTrue(removed.isEmpty());
        }
        {
            Range<Integer> r5_12 = new Range<>(5, true, 12, false);
            assertFalse(r10_5.contains(r5_12));
            assertFalse(r5_12.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r5_12, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, 12, true, 5, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(10, true, 12, false)));
        }
        {
            Range<Integer> r5_0 = new Range<>(true, 5, true, 0, false);
            assertFalse(r10_5.contains(r5_0));
            assertFalse(r5_0.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r5_0, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, 0, true, 5, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(true, 10, true, 0, false)));
        }
        // subtrahend min = 8, vary max
        {
            Range<Integer> r8_9 = new Range<>(8, true, 9, false);
            assertFalse(r10_5.contains(r8_9));
            assertFalse(r8_9.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r8_9, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, 10, true, 5, false)));
            assertTrue(removed.isEmpty());
        }
        {
            Range<Integer> r8_10 = new Range<>(8, true, 10, false);
            assertFalse(r10_5.contains(r8_10));
            assertFalse(r8_10.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r8_10, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, 10, true, 5, false)));
            assertTrue(removed.isEmpty());
        }
        {
            Range<Integer> r8_12 = new Range<>(8, true, 12, false);
            assertFalse(r10_5.contains(r8_12));
            assertFalse(r8_12.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r8_12, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, 12, true, 5, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(10, true, 12, false)));
        }
        {
            Range<Integer> r8_0 = new Range<>(true, 8, true, 0, false);
            assertFalse(r10_5.contains(r8_0));
            assertFalse(r8_0.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r8_0, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(0, true, 5, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(true, 10, true, 0, false)));
        }

        // subtrahend min = 10, vary max
        {
            Range<Integer> r10_10 = new Range<>(10, true, 10, true);
            assertTrue(r10_5.contains(r10_10));
            assertFalse(r10_10.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r10_10, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, 10, false, 5, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(10, true, 10, true)));
        }
        {
            Range<Integer> r10_0 = new Range<>(true, 10, true, 0, false);
            assertTrue(r10_5.contains(r10_0));
            assertFalse(r10_0.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r10_0, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(0, true, 5, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(true, 10, true, 0, false)));
        }
        {
            Range<Integer> r10_8 = new Range<>(true, 10, true, 8, false);
            assertFalse(r10_5.contains(r10_8));
            assertTrue(r10_8.contains(r10_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r10_5.retain(r10_8, removed);
            assertTrue(retain.isEmpty());
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(true, 10, true, 5, false)));
        }
    }

    @Test
    public void test3() {
        {
            Range<Integer> r5_5 = new Range<>(true, 5, true, 5, false);
            Range<Integer> r4_6 = new Range<>(true, 4, true, 6, false);
            assertTrue(r5_5.contains(r4_6));
            assertFalse(r4_6.contains(r5_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_5.retain(r4_6, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, 6, true, 4, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(4, true, 6, false)));
        }
        {
            Range<Integer> r4_6 = new Range<>(true, 4, true, 6, false);
            Range<Integer> r5_5 = new Range<>(true, 5, true, 5, false);
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r4_6.retain(r5_5, removed);
            assertTrue(retain.isEmpty());
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(4, true, 6, false)));
        }
        {
            Range<Integer> r5_5 = new Range<>(true, 5, true, 5, true);
            Range<Integer> r5_5c = new Range<>(true, 5, true, 5, true);
            assertTrue(r5_5.contains(r5_5c));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_5.retain(r5_5c, removed);
            assertTrue(retain.isEmpty());
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(5, true, 5, true)));
        }
        {
            Range<Integer> r5_5 = new Range<>(true, 5, true, 5, false);
            Range<Integer> r6_6 = new Range<>(true, 6, true, 6, true);
            assertTrue(r5_5.contains(r5_5));
            assertFalse(r6_6.contains(r5_5));
            List<Range<Integer>> removed = new ArrayList<>();
            List<Range<Integer>> retain = r5_5.retain(r6_6, removed);
            assertTrue(retain.size() == 1);
            assertTrue(retain.get(0).isSameRange(new Range<Integer>(true, 6, false, 6, false)));
            assertTrue(removed.size() == 1);
            assertTrue(removed.get(0).isSameRange(new Range<Integer>(6, true, 6, true)));
        }
    }
    @Test
    public void test4() {
        Range<Integer> r1 = new Range<>(true, 0, true, 10, false);
        Range<Integer> r2 = new Range<>(true, 10, true, 0, false);
        Range<Integer> r3 = new Range<>(true, 10, true, 0, true);
        Range<Integer> r4 = new Range<>(true, 10, false, 0, false);
        Range<Integer> r5 = new Range<>(true, 10, false, 0, true);

        assertFalse(r1.contains(r2));
        assertFalse(r2.contains(r1));
        assertFalse(r1.contains(r3));
        assertFalse(r3.contains(r1));
        assertFalse(r1.contains(r4));
        assertFalse(r4.contains(r1));
        assertFalse(r1.contains(r5));
        assertFalse(r5.contains(r1));
    }
}
