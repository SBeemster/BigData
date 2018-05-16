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
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.bson.BSONObject;
import org.bson.Document;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.util.JSON;

public class insertToMongoDB {

	public static void main(String[] args) {
		
		try {
			MongoCredential credential = MongoCredential.createCredential("rdwUser", "rdw", "welkom".toCharArray());
			MongoClient mongodb = new MongoClient(new ServerAddress("192.168.178.26", 27017), Arrays.asList(credential));
			DB db = mongodb.getDB("rdw");
	
			System.out.println("Connected");
		
			DBCollection voertuigCollection = db.getCollection("voertuig");
		
			String gekentekendeVoertuigenBrandstof = "csv/Open_Data_RDW__Gekentekende_voertuigen_brandstof.csv";
			String keuringen = "csv/Open_Data_RDW__Keuringen.csv";
			String meldingenKeuringsInstanties = "csv/Open_Data_RDW__Meldingen_Keuringsinstantie.csv";
        
			PrintWriter writer =null;
			try {
				writer = new PrintWriter(new FileOutputStream("txt/logFile.log", false));
				Date begintijd = new Date();
				writer.println("INFO: Programma voor invoer gestart. Beingtijd "+ begintijd);
				
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			BufferedReader br = null;
	        String line = "";
	        String cvsSplitBy = ",";

	        JSONObject json = new JSONObject();
	        DBObject doc;
        //CSV's omzetten naar bruikbare arrayisten      
        try {
			br = new BufferedReader(new FileReader(gekentekendeVoertuigenBrandstof));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        writer.println("INFO: Start kenteken controle RDW en invoer MongoDB");
	    System.out.println("INFO: Start kenteken controle RDW en invoer MongoDB");
        
        try {
        	int voertuigenBrandstofSize =0;
			while ((line = br.readLine()) != null) {
				
			    // gebruik komma voor split
			    String[] voertuigBrandstof = line.split(cvsSplitBy);
			    
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
	    		
	    		
	    		if(responseCode != 200) {
	    			writer.println("Error: Helaas is de verbinding verbroken. Validatie mislukt");
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
		    			kenteken = kentekenVerbeteren(kenteken);
		    			
			        	url = "https://opendata.rdw.nl/resource/m9d7-ebf2.json?kenteken="+kenteken;
			    		
			    		obj = new URL(url);
			    		con = (HttpURLConnection) obj.openConnection();

			    		// optional default is GET
			    		con.setRequestMethod("GET");

			    		//add request header
			    		con.setRequestProperty("User-Agent", "Mozilla/5.0");

			    		responseCode = con.getResponseCode();
			    		
			    		if(responseCode != 200) {
			    			writer.println("Error: Helaas is de verbinding verbroken. Validatie mislukt");
			    				break;
				    		}else {
				    		in = new BufferedReader(
				    		        new InputStreamReader(con.getInputStream()));
				    		
				    		response = new StringBuffer();
			
				    		while ((inputLine = in.readLine()) != null) {
				    			response.append(inputLine);
				    		}
				    		in.close();
				    		responseString = response.toString();
				    		
					    		if(responseString.isEmpty() || responseString.equals("") || responseString.equals("[]")) {
			    			
					    			writer.println("Error: Kenteken " + kenteken + " is ongeldig.");
					    			writer.flush();
					    		}
					    		else {
					    			
					    			json.put("kenteken", kenteken);
					    			json.put("brandstof omschrijving", voertuigBrandstof[2]);
					    			json.put("brandstofverbruik buiten de stad", voertuigBrandstof[3]);
					    			json.put("brandstofverbruik gecombineerd", voertuigBrandstof[4]);
					    			json.put("brandstofverbruik stad", voertuigBrandstof[5]);
					    			json.put("co2 uitstoot gecombineerd", voertuigBrandstof[6]);
					    			json.put("geluidsniveau rijdend", voertuigBrandstof[8]);
					    			json.put("geluidsniveau stationair", voertuigBrandstof[9]);
					    			json.put("emissiecode omschrijving", voertuigBrandstof[10]);
					    		
					    			
					    			doc = (DBObject) JSON.parse(json.toString());
					    			try {
					    				voertuigCollection.insert(doc);
						    			writer.println("INFO: Voertuig met kenteken: "+kenteken + " is in MongoDB geplaatst");
						    			}catch(Exception e){
						    				try {
						    					
						    					BasicDBObject query = new BasicDBObject();
						    					query.put("kenteken", kenteken);
						    			       	DBObject current = voertuigCollection.findOne(query);
								    			current.put("brandstof omschrijving", voertuigBrandstof[2]);
								    			current.put("brandstofverbruik buiten de stad", voertuigBrandstof[3]);
								    			current.put("brandstofverbruik gecombineerd", voertuigBrandstof[4]);
								    			current.put("brandstofverbruik stad", voertuigBrandstof[5]);
								    			current.put("co2 uitstoot gecombineerd", voertuigBrandstof[6]);
								    			current.put("geluidsniveau rijdend", voertuigBrandstof[8]);
								    			current.put("geluidsniveau stationair", voertuigBrandstof[9]);
								    			current.put("emissiecode omschrijving", voertuigBrandstof[10]);
								    			voertuigCollection.findAndModify(query, current);
						    					//voertuigCollection.findAndModify(query, doc);
						    					writer.println("INFO: Voertuig met kenteken: "+ kenteken + " is geupdate.");
						    				}catch(Exception ex){
						    					writer.println("ERROR: Voertuig niet geupdate vanwege error: "+ ex);
						    				}
						    			}
					    		}
				    		}
		    		}else {
		    		
		    			json.put("kenteken", kenteken);
		    			json.put("brandstof omschrijving", voertuigBrandstof[2]);
		    			json.put("brandstofverbruik buiten de stad", voertuigBrandstof[3]);
		    			json.put("brandstofverbruik gecombineerd", voertuigBrandstof[4]);
		    			json.put("brandstofverbruik stad", voertuigBrandstof[5]);
		    			json.put("co2 uitstoot gecombineerd", voertuigBrandstof[6]);
		    			json.put("geluidsniveau rijdend", voertuigBrandstof[8]);
		    			json.put("geluidsniveau stationair", voertuigBrandstof[9]);
		    			json.put("emissiecode omschrijving", voertuigBrandstof[10]);
		    		
		    			
		    			doc = (DBObject) JSON.parse(json.toString());
		    			
		    			try {
		    			voertuigCollection.insert(doc);
		    			writer.println("INFO: Voertuig met kenteken: "+kenteken + " in MongoDB geplaatst!");
		    			}catch(Exception e){
		    				try {
		    					
		    					BasicDBObject query = new BasicDBObject();
		    					query.put("kenteken", kenteken);
		    			       	DBObject current = voertuigCollection.findOne(query);
				    			current.put("brandstof omschrijving", voertuigBrandstof[2]);
				    			current.put("brandstofverbruik buiten de stad", voertuigBrandstof[3]);
				    			current.put("brandstofverbruik gecombineerd", voertuigBrandstof[4]);
				    			current.put("brandstofverbruik stad", voertuigBrandstof[5]);
				    			current.put("co2 uitstoot gecombineerd", voertuigBrandstof[6]);
				    			current.put("geluidsniveau rijdend", voertuigBrandstof[8]);
				    			current.put("geluidsniveau stationair", voertuigBrandstof[9]);
				    			current.put("emissiecode omschrijving", voertuigBrandstof[10]);
				    			voertuigCollection.findAndModify(query, current);
		    					//voertuigCollection.findAndModify(query, doc);
		    					writer.println("INFO: Voertuig met kenteken: "+ kenteken + " is geupdate.");
		    				}catch(Exception ex){
		    					writer.println("ERROR: Voertuig niet geupdate vanwege error: "+ ex);
		    				}
		    				
		    			}
		    			
		    		}
		    	
		        	}
	        	}
			    
			
			
			writer.println("INFO: Kenteken invoer afgerond");
			System.out.println("INFO: Kenteken invoer afgerond");
			writer.println("-------------------------------------------------------------------");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        writer.println("INFO: Start toevoegen keuringen aan kentekend bekend in MongoDB");
        System.out.println("INFO: Start toevoegen keuringen aan kentekend bekend in MongoDB");
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
				BasicDBObject query = new BasicDBObject();
				String kenteken = keuring[0];
				query.put("kenteken", kenteken);
		       	DBObject current = voertuigCollection.findOne(query);
		       	
		       	if(current == null) {
		       		writer.println("Error: Kenteken "+ kenteken +" niet gevonden in de MongoDB!");
		       	}else {
		       		List<BasicDBObject> keuringenVoertuig= new ArrayList<>();
		       		keuringenVoertuig.add(new BasicDBObject("vervaldatum", keuring[1]));
		       		current.put("keuringen", keuringenVoertuig);
		       		try {
		       			voertuigCollection.findAndModify(query, current);
		       			writer.println("Info: Keuring toegevoegd aan voertuig met kenteken: "+ kenteken);
		       		}catch(Exception e) {
		       			writer.println("Error: Keuring niet toegevoegd aan voertuig met kenteken: "+ kenteken);
		       		}
		       	}
			}
			writer.println("INFO: Einde toevoegen keuringen aan MongoDB");
			System.out.println("INFO: Einde toevoegen keuringen aan MongoDB");
			writer.println("---------------------------------------------------");
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
         try {
			br = new BufferedReader(new FileReader(meldingenKeuringsInstanties));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        writer.println("INFO: Start toevoegen meldingen keuringsinstanties aan kentekens bekend in MongoDB");
     	System.out.println("INFO: Start toevoegen meldingen keuringsinstanties aan kentekens bekend in MongoDB");
        try {
        	
			while ((line = br.readLine()) != null) {
				
			    // gebruik komma voor split
			    String[] meldingKeuringsInstantie = line.split(cvsSplitBy);
			    BasicDBObject query = new BasicDBObject();
				String kenteken = meldingKeuringsInstantie[0];
				query.put("kenteken", kenteken);
		       	DBObject current = voertuigCollection.findOne(query);
		       	
		       	if(current == null) {
		       		writer.println("Error: Kenteken "+ kenteken +" niet gevonden in de MongoDB!");
		       	}else {
		       		List<BasicDBObject> keuringsMeldingenVoertuig= new ArrayList<>();
		       		BasicDBObject keuringsinstantiemelding = new BasicDBObject();
		       		keuringsinstantiemelding.put("Soort erkenning keuringsinstantie", meldingKeuringsInstantie[1]);
		       		keuringsinstantiemelding.put("Meld datum door keuringsinstantie", meldingKeuringsInstantie[2]);
		       		keuringsinstantiemelding.put("Meld tijd door keuringsinstantie", meldingKeuringsInstantie[3]);
		       		keuringsinstantiemelding.put("Vervaldatum keuring", meldingKeuringsInstantie[6]);
		       		keuringsinstantiemelding.put("Steekproef indicator", meldingKeuringsInstantie[7]);
		       		
		       		keuringsMeldingenVoertuig.add(keuringsinstantiemelding);
		       		current.put("keuringen", keuringsMeldingenVoertuig);
		       		try {
		       			voertuigCollection.findAndModify(query, current);
		       			writer.println("Info: Keuringsmelding van instantie toegevoegd aan voertuig met kenteken: "+ kenteken);
		       		}catch(Exception e) {
		       			writer.println("Error: Keuringsmelding van instantie niet toegevoegd aan voertuig met kenteken: "+ kenteken);
		       		}
		       	}
			    
			
			}
			writer.println("INFO: Einde toeveogen keuringsinstanties aan kentekens bekend in MongoDB");
			System.out.println("INFO: Einde toeveogen keuringsinstanties aan kentekens bekend in MongoDB");
			writer.println("-------------------------------------------------------------------------");
			
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
        
		

		Date eindtijd = new Date();
		
        writer.println("Info: Einde invoer programma. Eindtijd: " + eindtijd);
		writer.close();
        
		}catch(Exception e){
			System.out.println(e);
		}
}

	private static String kentekenVerbeteren(String kenteken) {
		kenteken = kenteken.replace("-", "");
		
		return kenteken;
	}
}