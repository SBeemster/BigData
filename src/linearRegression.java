import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class linearRegression {

	public static void main(String[] args) {
		//MongoCredential credential = MongoCredential.createCredential("rdwUser", "rdw", "welkom".toCharArray());
		MongoClient mongodb = new MongoClient("192.168.216.134", 27017);
		DB db = mongodb.getDB("rdw");
		
		DBCollection toelatingenPerJaar = db.getCollection("toelating_per_jaar");
		
	
		int aantal = (int) toelatingenPerJaar.count();
		
		double[][] regressionArray = new double[aantal][2];
		
		List<DBObject> items =toelatingenPerJaar.find().toArray();
				
			try {
				for(int i=0; i < items.size(); i++) {
					JSONObject JSON = new JSONObject(items.get(i).toString());
					//Alleen jaren toevoegen groter dan 2000 i.v.m. enorme stijging in deze jaren.
					if(JSON.get("_id") != "NaN" &&  (int) JSON.getDouble("_id") > 2000 &&  (int) JSON.getDouble("_id") < 2018) {
						
						regressionArray[i][0] = (int) JSON.getDouble("_id");
						
						regressionArray[i][1] = (int) JSON.getDouble("value");
					}
				}
				
				SimpleRegression regression = new SimpleRegression();
				regression.addData(regressionArray);
				
				System.out.println("Intercept: "+ regression.getIntercept());
				System.out.println("Slope: " + regression.getSlope());
				System.out.println("Slope Standard Error: " +regression.getSlopeStdErr());
				
				double V2040 = regression.getIntercept() + regression.getSlope() * 2040;
				System.out.println("Voorspelling voor het jaar 2040 is het aantal voertuigen: " + V2040);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	}

}
