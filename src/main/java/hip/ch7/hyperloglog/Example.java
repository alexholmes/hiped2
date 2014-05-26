package hip.ch7.hyperloglog;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import net.agkn.hll.HLL;

/**
 * Created by aholmes on 5/25/14.
 */
public class Example {

public static void main(String... args) {

HashFunction hasher = Hashing.murmur3_128();

final Integer[] data = new Integer[]{1, 1, 2, 2, 3, 3, 4, 4, 5, 5};

final HLL hll = new HLL(13/*log2m*/, 5/*registerWidth*/);

for (int item : data) {
  final long hashedValue = hasher.newHasher().putInt(item).hash().asLong();
  hll.addRaw(hashedValue);
}

System.out.println("Distinct count = " + hll.cardinality());

}
}
