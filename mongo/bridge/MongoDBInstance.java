package com.techstudio.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class MongoDBInstance {
	private MongoClient _client = null;
	private MongoDatabase _db = null;
	public MongoDBInstance(MongoClient client,MongoDatabase db){
		_client = client;
		_db = db;
	}
	
	public MongoDatabase getDB() {
		return _db;
	}

	public void close(){
		_client.close();
	}
}
