package com.knoldus.apache.bean.file.processing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.log4j.Logger;

class User extends SimpleFunction<String, String> {

	@Override
	public String apply(String input) {
		// it is map element transformation, so this method is called for each element
		// of file
		// Each element of file is in the form of string input,so we can to split the
		// file row
		// using file splitter
		String[] str = input.split(",");
		String sessionId = str[0];
		String userId = str[1];
		String userName = str[2];
		String videoId = str[3];
		String duration = str[4];
		String startTime = str[5];
		String sex = str[6];
		String output = "";
		if ("1".equals(sex)) {
			output = sessionId + "," + userId + "," + userName + "," + videoId + "," + duration + "," + startTime + ","+ "M";
		} else if ("2".equals(sex)) {
			output = sessionId + "," + userId + "," + userName + "," + videoId + "," + duration + "," + startTime + "," + "F";
		} else {
			output = input;
		}

		return output;
	}
}

public class MapElementsSimpleFunctionTestMain {
	
	private static final Logger LOGGER = Logger.getLogger(MapElementsSimpleFunctionTestMain.class);

	public static void main(String[] args) {
		// create pipeline
		LOGGER.info("creating pipeline");
		Pipeline pipeline = Pipeline.create();

		// reading file using file line
		LOGGER.info("reading file");
		PCollection<String> inputFileApply = pipeline
				.apply(TextIO.read().from("src/main/resources/input_files/user.csv"));
		
		//transforming file element using simple function
		LOGGER.info("tranforming file element using simple function");
		PCollection<String> transformedApply = inputFileApply.apply(MapElements.via(new User()));
		
		//write transformed element into a file
		LOGGER.info("writing file");
		transformedApply.apply(
				TextIO.write()
				      .to("src/main/resources/output_files/output_user_csv")
				      .withNumShards(1)
				      .withSuffix(".csv")
				);
		
		//run the pipeline
		LOGGER.info("starting pipeline");
		pipeline.run().waitUntilFinish();
		

	}

}
