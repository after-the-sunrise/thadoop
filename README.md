# Thadoop
Thadoop (Thrift + Hadoop) is a simple set of wrapper templates to integrate Apache Thrift implementation with Hadoop's Writable interface. 

The goal of this module is to provide a quick and easy way to use auto-generate thrift codes as a Input/Output for the Hadoop related tasks.

## Prerequisites
* JDK 1.7+
* Maven 3.0.4+
* Working Thrift generated codes
* Working Hadoop Environment

  (Sample idl `src/thrift/idl/thadoop.thrift` and generated Java codes `src/thrift/java/*` are included for convenience.)

## Maven dependency configuration
In the maven project file, add maven repository and dependency to retrieve the thadoop module.

[pom.xml]

	<repositories>
		<repository>
			<id>rp.after-sunrise_release</id>
			<name>after-sunrise Release</name>
			<url>https://rp.after-sunrise.com/maven/release</url>
		</repository>
		<repository>
			<id>rp.after-sunrise_snapshot</id>
			<name>after-sunrise Snapshot</name>
			<url>https://rp.after-sunrise.com/maven/snapshot</url>
		</repository>
	</repositories>
	
	<dependencies>
		<dependency>
			<groupId>com.after_sunrise.oss</groupId>
			<artifactId>thadoop</artifactId>
			<version>${LATEST_VERSION_HERE}</version>
		</dependency>
	</dependencies>

## Implementation

### TWritable
This is the very base of all the other integrations. Implement a subclass of the custom `TWritable` first. (Swap `ThadoopSample` class in the sample below.)

[SampleWritable.java]

	public class SampleWritable extends TWritable<ThadoopSample> {
		
		private final ThadoopSample base = new ThadoopSample();
		
		@Override
		public ThadoopSample get() {
			return base; // Note : Do NOT create new instance here !!!
		}
		
	}

This subclass can be fed to Mapper/Reducer directly, as the superclass implements the Hadoop's writable interface.

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
Create a subclass of `TStorage`. This superclass implements the Pig's load function. 
* Handles thrift records stored in Hadoop's Sequence file format.
* Key is ignored, and only the value will be parsed.

Subclass implementation should look like something below:

[SampleStorage.java]

	public class SampleStorage extends TStorage<ThadoopSample._Fields, SampleWritable> {
		
		public SampleStorage() {
			super(SampleWritable.class, ThadoopSample.metaDataMap);
		}
		
	}


### Hive SerDe
Create a subclass of `TSerDe`. This superclass implements the Hive's SerDe interface.

[SampleSerDe.java]

	public class SampleSerDe extends TSerDe {

		public SampleSerDe() {
			super(SampleWritable.class);
		}

	}
