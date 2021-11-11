package com.knoldus.apache.bean.file.processing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.log4j.Logger;

public class FlatternExampleMainTest {
	
	private static final Logger LOGGER = Logger.getLogger(FlatternExampleMainTest.class);

	public static void main(String[] args) {
		//create pipeline 
		LOGGER.info("creating pipeline");
		Pipeline pipeline = Pipeline.create();
		
		//read file using pipeline
		LOGGER.info("reading file");
		PCollection<String> firstCustomerFileApply = pipeline.apply(TextIO
				.read()
				.from("src/main/resources/input_files/customer_1.csv")
				);
		
		PCollection<String> secondCustomerFileApply = pipeline.apply(TextIO
				.read()
				.from("src/main/resources/input_files/customer_2.csv")
				);
		
		PCollection<String> thirdCustomerFileApply = pipeline.apply(TextIO
				.read()
				.from("src/main/resources/input_files/customer_3.csv")
				);
		
		//merging all file into a single file
		LOGGER.info("merging all files into single file");
		PCollectionList<String> mergedFileList = PCollectionList.of(firstCustomerFileApply).and(secondCustomerFileApply).and(thirdCustomerFileApply);

		//flattening the all file records into a single file
		PCollection<String> flattenFileRecords = mergedFileList.apply(Flatten.pCollections());
		
		//writing flatten file into a file
		//writing the PCollection element to a file
		LOGGER.info("writing file");
		flattenFileRecords.apply(TextIO
						.write()
						.to("src/main/resources/output_files/merged_customer.csv")
						.withHeader("id,name,lastName,city")
						.withNumShards(1)
						.withSuffix(".csv")
						);
	    //run the pipeline
		LOGGER.info("starting pipeline");
		pipeline.run().waitUntilFinish();
		
		
	}

}
