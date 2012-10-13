# Thadoop
Thadoop is a simple set of wrapper templates to integrate Apache Thrift implementation with Hadoop's Writable interface.

## Prerequisites
* JDK 1.7+
* Maven 3.0+
* Working Thrift generated codes
* Working Hadoop Environment
* (Preferred) Eclipse 3.7+ 

  (Sample idl `src/thrift/idl/thadoop.thrift` and codes `src/thrift/gen/*` are included for convenience.)

## Maven dependency configuration
In your maven project file, add thadoop module's maven repository and dependency.

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
	
	<dependencies>
		<dependency>
			<groupId>jp.gr.java_conf.afterthesunrise</groupId>
			<artifactId>thadoop</artifactId>
			<version>${LATEST_VERSION_HERE}</version>
		</dependency>
	</dependencies>

## Implementation

### Writable
This is the very base of all the other integrations. You will need to prepare your own thrift beforehand. Once you have generated your thrift implementation, create a subclass of `AbstractTWritable.java` similar to the sample below. (Swap 'ThadoopSample' with your thrift class)

[SampleWritable.java]

	public class SampleWritable extends AbstractTWritable<ThadoopSample> {
		
		private final ThadoopSample base = new ThadoopSample();
		
		@Override
		public ThadoopSample get() {
			return base;
		}
		
	}

You can feed this subclass to Mapper/Reducer directly, as the superclass implements the Hadoop's writable interface.

[SampleJob.java]

	public class SampleJob extends Configured implements Tool {

		@Override
		public int run(String[] args) throws Exception {
			
			Job job = ...
			
			job.setOutputValueClass(SampleWritable.class);
			
			...
			
		}
		
	}


### Pig Storage
Create a subclass of `AbstractTStorage.java`. This superclass implements the Pig's load function. 
* Handles thrift records stored in Hadoop's Sequence file format.
* Key is ignored, and only the value will be parsed.

Subclass implementation should look like something below:

[SampleStorage.java]

	public class SampleStorage extends AbstractTStorage<ThadoopSample._Fields, SampleWritable> {
		
		public SampleStorage() {
			super(SampleWritable.class, ThadoopSample.metaDataMap);
		}
		
	}


### Hive SerDe
Create a subclass of `AbstractTSerDe.java`. This superclass implements the Hive's SerDe interface.

[SampleSerDe.java]

	public class SampleSerDe extends AbstractTSerDe<ThadoopSample._Fields, SampleWritable> {

		public SampleSerDe() {
			super(SampleWritable.class, ThadoopSample.metaDataMap);
		}

	}
