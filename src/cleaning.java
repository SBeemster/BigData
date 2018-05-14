import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

public class cleaning {

	public static void main(String[] args) {
		String gekentekendeVoertuigenBrandstof = "csv/Open_Data_RDW__Gekentekende_voertuigen_brandstof.csv";
		String keuringen = "csv/Open_Data_RDW__Keuringen.csv";
		String meldingenKeuringsInstanties = "csv/Open_Data_RDW__Meldingen_Keuringsinstantie.csv";
        
		BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        ArrayList<String[]> voertuigenList = new ArrayList<String[]>();
        
        //CSV's omzetten naar bruikbare arrayisten
 
        
        ArrayList<String[]> voertuigenBrandstofList = new ArrayList<String[]>();
        try {
			br = new BufferedReader(new FileReader(gekentekendeVoertuigenBrandstof));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			while ((line = br.readLine()) != null) {
				
			    // gebruik komma voor split
			    String[] voertuigBrandstof = line.split(cvsSplitBy);
			    voertuigenBrandstofList.add(voertuigBrandstof);
			    System.out.println(line);
			 
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        ArrayList<String[]> keuringenList = new ArrayList<String[]>();
        try {
			br = new BufferedReader(new FileReader(keuringen));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        try {
			while ((line = br.readLine()) != null) {
				
			    // gebruik komma voor split
			    String[] keuring = line.split(cvsSplitBy);
			    keuringenList.add(keuring);
			    System.out.println(line);
			
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        ArrayList<String[]> meldingenKeuringsInstantiesList = new ArrayList<String[]>();
        try {
			br = new BufferedReader(new FileReader(meldingenKeuringsInstanties));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			while ((line = br.readLine()) != null) {
				
			    // gebruik komma voor split
			    String[] meldingKeuringsInstantie = line.split(cvsSplitBy);
			    meldingenKeuringsInstantiesList.add(meldingKeuringsInstantie);
			    System.out.println(line);
			
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        //Data analyse en controle
        try {
			PrintWriter writer = new PrintWriter(new FileOutputStream("txt/logFile.log", false));
			//Tel het aantal voertuigen zoals bekend in de verschillende csv's
	        writer.println("Het aantal voertuigen met in de brandstoffen lijst is:"+ voertuigenBrandstofList.size());
	        
	        for(int i=0; i < voertuigenBrandstofList.size(); i++) {
	        	String[] voertuig = voertuigenBrandstofList.get(i);
	        	String kenteken = voertuig[0];
	        	String url = "https://opendata.rdw.nl/resource/m9d7-ebf2.json?kenteken="+kenteken;
	    		
	    		URL obj = new URL(url);
	    		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

	    		// optional default is GET
	    		con.setRequestMethod("GET");

	    		//add request header
	    		con.setRequestProperty("User-Agent", "Mozilla/5.0");

	    		int responseCode = con.getResponseCode();
	    		System.out.println("\nSending 'GET' request to URL : " + url);
	    		System.out.println("Response Code : " + responseCode);
	    		if(responseCode != 200) {
	    			writer.println("Helaas is de verbinding verbroken. Validatie mislukt");
	    				break;
		    		}else {
		    		BufferedReader in = new BufferedReader(
		    		        new InputStreamReader(con.getInputStream()));
		    		String inputLine;
		    		StringBuffer response = new StringBuffer();
	
		    		while ((inputLine = in.readLine()) != null) {
		    			response.append(inputLine);
		    		}
		    		in.close();
		    		String responseString = response.toString();
		    		
		    		if(responseString.isEmpty() || responseString.equals("") || responseString.equals("[]")) {
		    			writer.println("Kenteken " + kenteken + " is ongeldig.");
		    		}
		    		System.out.println(responseString);
		        	}
	        	}
	        
	        writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
}
}