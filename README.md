# Thadoop
Thadoop is a simple set of template classes to integrate Apache Thrift classes with Hadoop's Writable interface.

## Prerequisites
* JDK 1.6+
* Maven 3.0+
* Working Thrift generated codes
* Working Hadoop Environment
* (Preferred) Eclipse 3.7+ 

  (Sample idl `src/thrift/idl/thadoop.thrift` and codes `src/thrift/gen/*` are included for convenience.)

## Maven configuration
In your maven project, add thadoop module maven repository and dependency.

[pom.xml]

	<repositories>
		<repository>
			<id>ats-release</id>
   			<name>CloudBees ats-release</name>
   			<url>https://repository-ats.forge.cloudbees.com/release/</url>
		</repository>
		<repository>
			<id>ats-snapshot</id>
			<name>CloudBees ats-snapshot</name>
			<url>https://repository-ats.forge.cloudbees.com/snapshot/</url>
		</repository>
	</repositories>
	...
	<dependencies>
		<dependency>
			<groupId>jp.gr.java_conf.afterthesunrise</groupId>
			<artifactId>thadoop</artifactId>
			<version>${LATEST_VERSION_HERE}</version>
		</dependency>
	</dependencies>

## Implementation
Create a subclass of `AbstractTWritable.java`. This superclass contains the actual implementation of the Hadoop's writable interface, so you can feed your subclass to Mapper/Reducer directly.  Subclassed implementation should something like below:

[SampleWritable.java]

	public class SampleWritable extends AbstractTWritable<ThadoopSample> {
		
		private final ThadoopSample base = new ThadoopSample();
		
		@Override
		public ThadoopSample get() {
			return base;
		}
		
	}

