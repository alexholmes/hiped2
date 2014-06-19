package hip.ch8.mrunit;

import org.apache.hadoop.mrunit.TestDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class MRUnitJUnitAsserts {

  public static <K, V, T extends TestDriver<K, V, T>> void assertOutputs(
      TestDriver<K, V, T> driver, List<Pair<K, V>> actuals) {

    List<Pair<K, V>> expected = driver.getExpectedOutputs();

    assertEquals("Number of expected records don't match actual number",
        expected.size(), actuals.size());

    // make sure all actual outputs are in the expected set,
    // and at the proper position.
    for (int i = 0; i < expected.size(); i++) {
      Pair<K, V> actual = actuals.get(i);
      Pair<K, V> expect = expected.get(i);
      assertEquals("Records don't match at position " + i,
          expect, actual);
    }
  }
}
