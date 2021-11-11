package com.knoldus.apache.bean.file.processing;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.log4j.Logger;

import com.knoldus.apache.bean.file.processing.model.CustomerEntity;

public class InMemoryExampleMainTest {
	
	private static final Logger LOGGER = Logger.getLogger(InMemoryExampleMainTest.class);

	public static void main(String[] args) {
		//creating the pipeline
		LOGGER.info("creating pipeline");
		Pipeline pipeline = Pipeline.create();
		
		//creating PCollection of CustomerEntity type
		PCollection<CustomerEntity> pCollectionCustomerEntity = pipeline.apply(Create.of(getCustomerEntityList()));
		
		//convert PCollection customer entity into PCollection String type
		PCollection<String> pcollectionString = pCollectionCustomerEntity.apply(
				MapElements.into(TypeDescriptors.strings())
				.via((CustomerEntity c) -> c.getName())
				);
		
		LOGGER.info("writing file");
		pcollectionString.apply(
				TextIO.write()
				       .to("src/main/resources/output_files/customer_info.csv")
				       .withNumShards(1)
				       .withSuffix(".csv")
				
				);
		LOGGER.info("starting pipeline");
		pipeline.run().waitUntilFinish();

	}

	public static List<CustomerEntity> getCustomerEntityList() {
		return Arrays.asList(new CustomerEntity("1001", "John"), new CustomerEntity("1002", "Adam"));

	}

}
