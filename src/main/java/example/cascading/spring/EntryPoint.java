package example.cascading.spring;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.ArrayUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.operation.regex.RegexGenerator;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import example.cascading.spring.domain.Bar;
import example.cascading.spring.domain.Foo;

public class EntryPoint {

	public static final String[] ORDERED_DST = new String[] {
		Bar.ID, Foo.COL1, Bar.COL2, Foo.COL3
	};

	public static final Fields FOO_FIELDS = new Fields(Foo.ORDERED_FIELDS);
	public static final Fields BAR_FIELDS = new Fields(Bar.ORDERED_FIELDS);
	public static final Fields DST_FIELDS = new Fields(ORDERED_DST);

	public static void main(String... args) throws IOException {

		ApplicationContext context = new ClassPathXmlApplicationContext(new String[]{ "context.xml" });

		Tap fooSrc = context.getBean("fooTap", Tap.class);
		Tap barSrc = context.getBean("barTap", Tap.class);
		Tap dest = context.getBean("destTap", Tap.class);

		Pipe foos = buildReadFoo(fooSrc);
		Pipe bars = buildReadBar(barSrc);
		Pipe join = buildJoin(foos, bars);

		FlowConnector.setApplicationJarClass(new Properties(), EntryPoint.class);
		FlowConnector fc = new FlowConnector();

		Map<String, Tap> sources = new HashMap<String, Tap>();
		sources.put(foos.getName(), fooSrc);
		sources.put(bars.getName(), barSrc);

		Flow f = fc.connect(sources, dest, join);
		f.complete();
	}

	public static Pipe buildReadFoo(Tap source) {
		Pipe p = new Pipe("Read Foos");
		p = new Each(p, new RegexGenerator("(.+),(.+),(.+),(.+)"), Fields.RESULTS);
		p = new Each(p, new Identity(FOO_FIELDS));

		return p;
	}

	public static Pipe buildReadBar(Tap source) {
		Pipe p = new Pipe("Read Bars");
		p = new Each(p, new RegexGenerator("(.+),(.+),(.+)"), Fields.RESULTS);
		p = new Each(p, new Identity(BAR_FIELDS));

		return p;
	}

	public static Pipe buildJoin(Pipe foos, Pipe bars) {
		Fields merged = new Fields((String[])ArrayUtils.addAll(Foo.ORDERED_FIELDS, Bar.ORDERED_FIELDS));

		Pipe p = new Pipe("Join Foo, Bar, on ID");
		p = new CoGroup(foos, new Fields(Foo.ID), bars, new Fields(Bar.ID), merged);
		p = new Each(p, DST_FIELDS, new Identity());

		return p;
	}
}
