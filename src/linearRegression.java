import java.util.List;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.json.JSONException;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


public class linearRegression {

	public static void main(String[] args) {
		MongoClient mongodb = new MongoClient("192.168.216.134", 27017);
		DB db = mongodb.getDB("rdw");	
		DBCollection toelatingenPerJaar = db.getCollection("toelating_per_jaar");
			
		int aantal = (int) toelatingenPerJaar.count();
		//Alleen jaren toevoegen groter dan 2006 i.v.m. enorme stijging in deze jaren.
		BasicDBObject query = new BasicDBObject();
		query.put("_id", new BasicDBObject("$gt", 2006).append("$lt", 2018));
		
		List<DBObject> items =toelatingenPerJaar.find(query).toArray();
		double[][] regressionArray = new double[items.size()][2];		
				
			for(int i=0; i < items.size(); i++) {
				double jaar = (double) items.get(i).get("_id");
				regressionArray[i][0] = (int) jaar;
				double value = (double) items.get(i).get("value");
				regressionArray[i][1] =(int) value;
			}

			SimpleRegression regression = new SimpleRegression();
			
			regression.addData(regressionArray);
			System.out.println("Intercept: "+ regression.getIntercept());
			System.out.println("Slope: " + regression.getSlope());
			System.out.println("Slope Standard Error: " +regression.getSlopeStdErr());
			
			
			System.out.println("Voorspelling voor het jaar 2040 is het aantal voertuigen: " + regression.predict(2040));		
	}
}
