package jp.gr.java_conf.afterthesunrise.thadoop;

import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import jp.gr.java_conf.afterthesunrise.thadoop.sample.ThadoopSample;
import jp.gr.java_conf.afterthesunrise.thadoop.sample.ThadoopSample._Fields;

import org.junit.Test;

/**
 * @author takanori.takase
 */
public class TFieldIdEnumComparatorTest {

	@Test
	public void testCompare() {

		List<ThadoopSample._Fields> list = new ArrayList<>();

		_Fields[] fields = _Fields.values();

		list.addAll(Arrays.asList(fields));

		Collections.shuffle(list);

		Collections.sort(list, TFieldIdEnumComparator.get());

		for (int i = 0; i < fields.length; i++) {
			assertSame(fields[i], list.get(i));
		}

	}

}
