package com.techstudio.mongo;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.mongodb.DBRef;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
@Component
public class MongoDBConnector {
	@Value("${mongodb.host}")
	private String host;
	@Value("${mongodb.port}")
	private int port;
	@Value("${mongodb.username}")
	private String user;
	@Value("${mongodb.password}")
	private char[] password;
	@Value("${mongodb.database}")
	private String database;
	// ...
	//private static Level DEBUG = org.apache.log4j.Level.DEBUG;
	private static Level INFO = org.apache.log4j.Level.INFO;
	private static int loggerState = 1;
	private static Level LEVEL = INFO;
	
	private MongoClient _client = null;
	private MongoDatabase _db = null;

	public static void stopLogger(){
		if (loggerState>0){
			LogManager.getLogger("org.mongodb.driver.connection").setLevel(LEVEL);
	        LogManager.getLogger("org.mongodb.driver.management").setLevel(LEVEL);
	        LogManager.getLogger("org.mongodb.driver.cluster").setLevel(org.apache.log4j.Level.OFF);
	        LogManager.getLogger("org.mongodb.driver.protocol.insert").setLevel(LEVEL);
	        LogManager.getLogger("org.mongodb.driver.protocol.query").setLevel(LEVEL);
	        LogManager.getLogger("org.mongodb.driver.protocol.update").setLevel(LEVEL);
	        loggerState = 0;
		}

	}
	public MongoClient getNewClient(){
		stopLogger();
		ServerAddress address= new ServerAddress(host,port);
		MongoCredential credential = MongoCredential.createScramSha1Credential(user, database, password);
		List<MongoCredential> mcs = new ArrayList<MongoCredential>();
		mcs.add(credential);
		MongoClient newClient = null;
		try{
			newClient = new MongoClient(address,mcs);
		}catch(Exception e){
			return null;
		}
		return newClient;
	}
	public MongoDatabase getNewDBbyClient(MongoClient client){
		return client.getDatabase(database);
	}
	
	public MongoClient getClient(){
		if (_client == null){
			_client = getNewClient();
		}
		return _client;
	}
	public MongoDatabase getDB(){
		if (_db == null){
			_db = getClient().getDatabase(database);
		}
		return _db;
	}
	
	public MongoDBInstance getInstance(){
		MongoClient n_c = getNewClient();
		MongoDatabase n_db = getNewDBbyClient(n_c);
		MongoDBInstance instance = new MongoDBInstance(n_c, n_db);
		return instance;
	}
	
	public Document fetch(DBRef ref){
		MongoCollection<Document> coll = getDB().getCollection(ref.getCollectionName());
		Bson filter= MongoUtil.getFilterByID(ref.getId().toString());
		Document result = coll.find(filter).first();
		return result;
	}
}
