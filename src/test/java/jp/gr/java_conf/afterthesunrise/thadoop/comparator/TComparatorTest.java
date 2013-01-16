package jp.gr.java_conf.afterthesunrise.thadoop.comparator;

import static jp.gr.java_conf.afterthesunrise.thadoop.sample.ThadoopSample._Fields.FIELD_INT;
import static jp.gr.java_conf.afterthesunrise.thadoop.sample.ThadoopSample._Fields.FIELD_LONG;
import static jp.gr.java_conf.afterthesunrise.thadoop.sample.ThadoopSample._Fields.FIELD_SHORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import jp.gr.java_conf.afterthesunrise.thadoop.sample.ThadoopSample;
import jp.gr.java_conf.afterthesunrise.thadoop.sample.ThadoopSample._Fields;

import org.junit.Before;
import org.junit.Test;

/**
 * @author takanori.takase
 */
public class TComparatorTest {

	private TComparator<_Fields> target;

	@Before
	public void setUp() throws Exception {
		target = new TComparator<_Fields>(FIELD_SHORT, FIELD_INT, FIELD_LONG);
	}

	@Test
	public void testCompare() {

		ThadoopSample s1 = new ThadoopSample();
		ThadoopSample s2 = new ThadoopSample();

		List<ThadoopSample> list = new ArrayList<ThadoopSample>();
		list.add(s1);
		list.add(s2);

		Collections.sort(list, target);

		assertEquals(2, list.size());

	}

	@Test
	public void testCompare_WithValues() {

		ThadoopSample s1 = new ThadoopSample();
		ThadoopSample s2 = new ThadoopSample();
		ThadoopSample s3 = new ThadoopSample();
		ThadoopSample s4 = new ThadoopSample();

		s1.setFieldShort((short) 2);
		s2.setFieldShort((short) 2);

		s2.setFieldInt(4);
		s3.setFieldInt(4);

		s3.setFieldLong(3L);
		s1.setFieldLong(3L);

		List<ThadoopSample> list = new ArrayList<ThadoopSample>();
		list.add(s1);
		list.add(s2);
		list.add(s3);
		list.add(s4);

		Collections.shuffle(list);

		Collections.sort(list, target);

		assertEquals(4, list.size());
		assertSame(s2, list.get(0));
		assertSame(s1, list.get(1));
		assertSame(s3, list.get(2));
		assertSame(s4, list.get(3));

	}

}
