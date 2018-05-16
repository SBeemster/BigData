import java.util.Arrays;
import java.util.List;

import org.bson.Document;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class showMongoDBItems {

	public static void main(String[] args) {
		MongoCredential credential = MongoCredential.createCredential("rdwUser", "rdw", "welkom".toCharArray());
		MongoClient mongodb = new MongoClient(new ServerAddress("192.168.178.26", 27017), Arrays.asList(credential));
		DB db = mongodb.getDB("rdw");
		
		DBCollection voertuigCollection = db.getCollection("voertuig");
		System.out.println("Collection: " + voertuigCollection);
		List<DBObject> items =voertuigCollection.find().toArray();
		voertuigCollection.drop();
		System.out.println("Aantal items in de voertuig collection: " + items.size());
		
		for(int i=0; i < items.size(); i++) {
			System.out.println(items.get(i).toString());
			
		}
	}

}
