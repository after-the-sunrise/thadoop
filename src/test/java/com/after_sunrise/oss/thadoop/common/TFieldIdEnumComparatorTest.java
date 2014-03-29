package com.after_sunrise.oss.thadoop.common;

import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.after_sunrise.oss.thadoop.sample.ThadoopSample;
import com.after_sunrise.oss.thadoop.sample.ThadoopSample._Fields;

/**
 * @author takanori.takase
 */
public class TFieldIdEnumComparatorTest {

	@Test
	public void testCompare() {

		List<ThadoopSample._Fields> list = new ArrayList<_Fields>();

		_Fields[] fields = _Fields.values();

		list.addAll(Arrays.asList(fields));

		Collections.shuffle(list);

		Collections.sort(list, TFieldIdEnumComparator.get());

		for (int i = 0; i < fields.length; i++) {
			assertSame(fields[i], list.get(i));
		}

	}

}
