package edu.csula.datascience.acquisition;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.scanner.Constant;
import com.sun.org.apache.xml.internal.utils.Constants;

public class MyAppSource {
    
	public void Download(String fileURL, String fileName){
		
		try {
			URL url = new URL(fileURL);
			FileUtils.copyURLToFile(url, new File("C:\\download\\"+fileName+".csv"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public File getDownloadedFile(String fileName, String directory){
		
		File dir = new File( directory );
		File[] inside_directory = dir.listFiles();
		
		//to check filename with csv		
		String check = fileName + ".csv";     
		boolean check_if_exist = new File(directory, check).exists();
		
		for(int i = 0; i < inside_directory.length; i++){
			if(inside_directory[i].getName().equals(fileName + ".csv")){
				//return "";
			}
			//return "";
		}
		
		
		return new File("C:\\download\\"+fileName);
	}
	
}
