# Thadoop [![Build Status](https://travis-ci.org/after-the-sunrise/thadoop.svg?branch=master)](https://travis-ci.org/after-the-sunrise/thadoop)
Thadoop (= Thrift + Hadoop) is a simple set of wrapper templates to integrate [Apache Thrift](https://thrift.apache.org/) implementation with Hadoop's Writable interface. 

The goal of this module is to provide a quick and easy way to use auto-generate thrift codes as a Input/Output for the Hadoop related tasks.

## Prerequisites
* JDK 1.7+
* Maven 3.0.4+
* Generated Thrift codes
* Hadoop Environment

  (Sample idl `src/thrift/idl/thadoop.thrift` and generated Java codes `src/thrift/java/*` are included for convenience.)


## Maven dependency configuration
In the maven project file, add maven repository and dependency to retrieve the thadoop module.

```xml
    <dependencies>
        <dependency>
            <groupId>com.after_sunrise.oss</groupId>
            <artifactId>thadoop</artifactId>
            <version>${LATEST_VERSION_HERE}</version>
        </dependency>
    </dependencies>
```

## Implementation

### TWritable
This is the very base of all the other integrations. Implement a subclass of the custom `TWritable` first. (Swap `ThadoopSample` class in the sample below.)

```java:SampleWritable.java
	public class SampleWritable extends TWritable<ThadoopSample> {
		
		private final ThadoopSample base = new ThadoopSample();
		
		@Override
		public ThadoopSample get() {
			return base; // Note : Do NOT create new instance here !!!
		}
		
	}
```

This subclass can be fed to Mapper/Reducer directly, as the superclass implements the Hadoop's writable interface.

```java:SampleJob.java
	public class SampleJob extends Configured implements Tool {

		@Override
		public int run(String[] args) throws Exception {
			
			Job job = ...
			
			job.setOutputValueClass(SampleWritable.class);
			
			...
			
		}
		
	}
```


### Pig Storage
Create a subclass of `TStorage`. This superclass implements the Pig's load function. 
* Handles thrift records stored in Hadoop's Sequence file format.
* Key is ignored, and only the value will be parsed.

Subclass implementation should look like something below:

```java:SampleStorage.java
	public class SampleStorage extends TStorage<ThadoopSample._Fields, SampleWritable> {
		
		public SampleStorage() {
			super(SampleWritable.class, ThadoopSample.metaDataMap);
		}
		
	}
```


### Hive SerDe
Create a subclass of `TSerDe`. This superclass implements the Hive's SerDe interface.

```java:SampleSerDe.java
	public class SampleSerDe extends TSerDe {

		public SampleSerDe() {
			super(SampleWritable.class);
		}

	}
```
