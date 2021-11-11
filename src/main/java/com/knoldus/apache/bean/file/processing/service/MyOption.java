package com.knoldus.apache.bean.file.processing.service;

import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOption extends PipelineOptions{
	
	public void setInputFile(String file);
	public String getInputFile();
	
	public void setOutputFile(String file);
	public String getOutputFile();
	
	public void setExtn(String extn);
	public String getExtn();
	

}
