package com.knoldus.apache.bean.file.processing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.log4j.Logger;

class MyCityPartition implements PartitionFn<String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int partitionFor(String element, int numPartitions) {
		
		if(element.contains("Los Angeles")) {
			return 0;
		}else if(element.contains("Phoenix")) {
			return 1;
		}else {
			return 2;
		}
		
	}
	
}

public class PartitionExampleMainTest {
	
	private static final Logger LOGGER = Logger.getLogger(PartitionExampleMainTest.class);

	public static void main(String[] args) {
		//create pipeline
		LOGGER.info("creating pipeline");
		Pipeline pipeline = Pipeline.create();
		
		//read file using pipeline 
		LOGGER.info("reading file");
		PCollection<String> partitionFileApply = pipeline.apply(TextIO
				.read()
				.from("src/main/resources/input_files/Partition.csv")
				);
		
		//creating partition of loaded file
		LOGGER.info("creating partition of loaded files");
		PCollectionList<String> partitionFileList = partitionFileApply.apply(Partition.of(3, new MyCityPartition()));
		
		//accessing the partition file from partition list
		LOGGER.info("getting data for file at index `0` of  partition files");
		PCollection<String> losAngelesFileRecord = partitionFileList.get(0);
		LOGGER.info("getting data for file at index `1` of  partition files");
		PCollection<String> phoneixFileRecord = partitionFileList.get(1);
		LOGGER.info("getting data for file at index `2` of  partition files");
		PCollection<String> newYorkFileRecord = partitionFileList.get(2);
		
		//writing the partitioned records into different different files
		LOGGER.info("writing file at index `0` of  partition files ");
		losAngelesFileRecord.apply(TextIO
				.write()
				.to("src/main/resources/output_files/los_angeles_partition.csv")
				.withHeader("id,name,lastName,city")
				.withNumShards(1)
				.withSuffix(".csv")
				);
		LOGGER.info("writing file at index `1` of  partition files ");
		phoneixFileRecord.apply(TextIO
				.write()
				.to("src/main/resources/output_files/phoneix_partition.csv")
				.withHeader("id,name,lastName,city")
				.withNumShards(1)
				.withSuffix(".csv")
				);
		
		LOGGER.info("writing file at index `2` of  partition files ");
		newYorkFileRecord.apply(TextIO
				.write()
				.to("src/main/resources/output_files/new_york_partition.csv")
				.withHeader("id,name,lastName,city")
				.withNumShards(1)
				.withSuffix(".csv")
				);
		
		//run the pipeline
		LOGGER.info("starting pipeline");
		pipeline.run().waitUntilFinish();

	}

}
