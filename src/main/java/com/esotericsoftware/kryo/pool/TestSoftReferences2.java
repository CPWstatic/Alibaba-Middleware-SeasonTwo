package com.esotericsoftware.kryo.pool;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.HashSet;

public class TestSoftReferences2 {
    public static void main(String args[]) {
        int count = 0;
        int collected = 0;
        ReferenceQueue q = new ReferenceQueue();
        HashSet<Reference> refs = new HashSet<Reference>();
        try {
            while (true) {
                for (int i = 0; i < 10; i++) {
                    byte junk[] = new byte[1000];
                }
                byte lump[] = new byte[1000];
                Reference ref = new SoftReference(lump, q);
                ref.get();
                refs.add(ref);
                count++;
                int lastCollected = collected;
                Reference queued = null;
                while ((queued = q.poll()) != null) {
                    refs.remove(queued);
                    collected++;
                }
                if (count % 10000 == 0) {
                    System.out.println("Created: " + count + ", collected: " + collected
                            + ", active: " + (count - collected));
                }
            }
        } finally {
            System.out.println("Created: " + count + ", Collected: " + collected);
        }
    }
}
