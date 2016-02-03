package com.techstudio.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.DBRef;
import com.mongodb.MongoClient;
import com.techstudio.util.FormatConvertUtil;

public class MongoUtil {
	
	public static List<Object> doclist2Objlist(List<Document> target){
		List<Object> result = new ArrayList<Object>();
		for (Document d : target){
			if (d!=null){
				result.add(MongoUtil.doc2Obj(d));	
			}
				
		}
		return result;
	}
	
	public static Object doc2Obj(Document d){
		CodecRegistry codecRegistry = CodecRegistries.fromRegistries(MongoClient.getDefaultCodecRegistry());
		final DocumentCodec codec = new DocumentCodec(codecRegistry, new BsonTypeClassMap());
		return FormatConvertUtil.Json2Object(d.toJson(codec));
	}
	
	public static List<Document> objlist2doclist(List<Object> target){
		List<Document> result = new ArrayList<Document>();
		for (Object o : target){
			result.add(MongoUtil.obj2doc(o));		
		}
		return result;
	}
	
	public static Document json2doc(String json){
		return Document.parse(json);
	}
	public static List<Document> json2doclist(String json){
		List<Object> list = FormatConvertUtil.Json2List(json);
		return objlist2doclist(list);
	}
	
	public static Document obj2doc(Object obj){
		String json = FormatConvertUtil.Obj2Json(obj);
		return Document.parse(json);
	}
	
	public static String getDocumentId(Document d){
		return d.get("_id").toString();
	}
	public static ObjectId getDocumentObjectId(Document d){
		return new ObjectId(getDocumentId(d));
	}
	
	public static DBRef createDBRefFromDocument(String collectionName,Document d){
		String id = getDocumentId(d);
		return new DBRef(collectionName, id);
		 
	}
	public static Bson getFilterByID(String id){
		Document d = new Document();
		d.put("_id", new ObjectId(id));
		return d;
	}
	public static Bson getFilterByMap(Map<String,?> map){
		Document d = new Document();
		for (Map.Entry<String, ?> entry : map.entrySet()) {
			d.put(entry.getKey(),entry.getValue());
		}
		return d;
	}
	public static void AppendID(Document d){
		d.append("id", getDocumentId(d));
	}
	public static void AppendID(Document d,String key){
		d.append(key, getDocumentId(d));
	}
}
