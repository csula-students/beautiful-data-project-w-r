package edu.csula.datascience.acquisition;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;

public class MyAppSource {
    
	public void Download(String fileURL, String fileName){
		
		try {
			URL url = new URL(fileURL);
			FileUtils.copyURLToFile(url, new File("C:\\download\\"+fileName+".csv"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public File getDownloadedFile(String fileName){
		return new File("C:\\download\\"+fileName);
	}
	
}
