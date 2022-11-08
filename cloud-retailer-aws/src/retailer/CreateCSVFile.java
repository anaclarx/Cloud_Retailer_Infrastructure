package retailer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import java.util.Enumeration;
import java.util.Hashtable;

public class CreateCSVFile {
	public static void writeDataLineByLine (String filePath) {
		
		 File file = new File(filePath);
		 Hashtable<String, String>[] myArray = new Hashtable[7];
		 Hashtable<String, String> ht = new Hashtable<>();
	      //Populating the array
		try {
			FileWriter outputfile = new FileWriter(file);
			CSVWriter writer = new CSVWriter(outputfile);
			String[] header = {"Store", "Store Profit", "Product", "Product Profit", "Product Quantity", "Product Sold"};
			Enumeration<String> eStore = myArray[0].keys();
			Enumeration<String> eProducts = myArray[1].keys();
			while(eProducts.hasMoreElements() || eStore.hasMoreElements()) {
				String keyStore = eStore.nextElement();
				String storeProfit = Double.toString(myArray[0].get(keyStore));
				String keyProduct = eProducts.nextElement();
				String productProfit = Double.toString(myArray[1].get(keyProduct));
				String productQuantity = Double.toString(myArray[2].get(keyProduct));
				String productSold = Double.toString(myArray[3].get(keyProduct));
				String[] data = {keyStore, storeProfit, keyProduct, productProfit, productQuantity, productSold};
				writer.writeNext(data);
			}
			writer.close();
		}
		   catch (IOException e) {
		        // TODO Auto-generated catch block
		        e.printStackTrace();
		    }
		
	}
	
}
