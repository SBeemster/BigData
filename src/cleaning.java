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
        
		PrintWriter writer =null;
		try {
			writer = new PrintWriter(new FileOutputStream("txt/logFile.log", false));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        
        //CSV's omzetten naar bruikbare arrayisten
 
        
        //ArrayList<String[]> voertuigenBrandstofList = new ArrayList<String[]>();
        try {
			br = new BufferedReader(new FileReader(gekentekendeVoertuigenBrandstof));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
        	int voertuigenBrandstofSize =0;
			while ((line = br.readLine()) != null) {
				
			    // gebruik komma voor split
			    String[] voertuigBrandstof = line.split(cvsSplitBy);
			    
			    System.out.println(line);
			    voertuigenBrandstofSize++;
			    
			    String kenteken = voertuigBrandstof[0];
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
		    			writer.flush();
		    		}
		    		System.out.println(responseString);
		        	}
	        	}
			    
			
			
			writer.println("Het aantal voertuigen met in de brandstoffen lijst is:"+ voertuigenBrandstofSize);
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
        
        //Controleer of kentekens die in de keuringenlijst staan ook in meldingen van keuringsinstanties voorkomen.
		writer.println("Het aantal gekeurde voertuigen is: "+ keuringenList.size());
		
		for(int i=0; i < keuringenList.size(); i++) {
			String[] keuring= keuringenList.get(i);
			boolean meldingGevonden = false;
			for(int j=0; j < meldingenKeuringsInstantiesList.size(); j++) {
				String[] meldingKeuring = meldingenKeuringsInstantiesList.get(j);
				if(keuring[0].equals(meldingKeuring[0])) {
					meldingGevonden = true;
					break;
				}
			}
			if(meldingGevonden == false) 
			{
				writer.println("Geen keuringsinstantie melding gevonden voor kenteken: "+keuring[0]);
			}
		}
		

		
		
		
		writer.close();
        
        
}
}