package com.techstudio.dao;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.mongodb.DBRef;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.techstudio.mongo.MongoDBConnector;
import com.techstudio.mongo.MongoDBInstance;
import com.techstudio.mongo.MongoUtil;

/**
 * @author xuekun.su@techstudio.com.sg
 *
 */
/**
 * @author tssdev2
 *
 */
abstract public class BaseMongoDao {

	private static final int LIMITSTRING = 255;

	@Autowired
	private MongoDBConnector mongo;

	private List<Object> _toList(List<Document> target) {
		return MongoUtil.doclist2Objlist(target);
	}

	protected static final Logger log = LoggerFactory.getLogger(BaseMongoDao.class);

	protected String _collectionName;

	protected MongoDBInstance getDBInstance() {
		return mongo.getInstance();
	}

	protected void logMessage(String message, int limit) {
		if (message == null)
			message = "no message";
		if (message.length() > limit)
			message = message.substring(0, limit);
		message = "|DBCollection |" + getCollectionName() + message;
		log.info(message);
	}

	protected void logMessage(String message) {
		logMessage(message, LIMITSTRING);
	}

	protected void errorMessage(String message, int limit) {
		if (message == null)
			message = "no message";
		if (message.length() > limit)
			message = message.substring(0, limit);
		message = "|DBCollection |" + getCollectionName() + message;
		log.error(message);
	}

	protected void errorMessage(String message) {
		errorMessage(message, LIMITSTRING);
	}

	public BaseMongoDao(String _name) {
		_collectionName = _name;
	}

	/**
	 * table name
	 * 
	 * @return
	 */
	public String getCollectionName() {
		return _collectionName;
	}

	// --------------------------------------------------
	// search functions
	// --------------------------------------------------
	
	/**
	 * 
	 * @return
	 */
	public long count() {
		MongoDBInstance ins = getDBInstance();
		MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
		long result = collection.count();
		ins.close();
		return result;
	}
	public long count(Bson filter) {
		MongoDBInstance ins = getDBInstance();
		MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
		long result = collection.count();
		ins.close();
		return result;
	}
	/**
	 * 
	 * @return
	 */
	public List<Document> listCollection() {
		MongoDBInstance ins = getDBInstance();
		MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
		List<Document> result = new ArrayList<Document>();
		collection.find().into(result);
		ins.close();
		return result;
	}

	public List<Document> listCollection(int limit) {
		List<Document> result = new ArrayList<Document>();
		if (limit == 0)
			return result;
		MongoDBInstance ins = getDBInstance();
		MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());

		collection.find().limit(limit).into(result);
		ins.close();
		return result;
	}

	public List<Document> listCollection(int from, int to) {

		List<Document> result = new ArrayList<Document>();
		if (from > to)
			return result;
		int limit = to - from + 1;
		if (from == 0)
			return listCollection(limit);

		MongoDBInstance ins = getDBInstance();
		MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());

		collection.find().limit(limit).skip(from).into(result);
		ins.close();
		return result;
	}

	public List<Document> listCollection(Bson sort, int limit) {
		MongoDBInstance ins = getDBInstance();
		MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
		List<Document> result = new ArrayList<Document>();
		collection.find().sort(sort).limit(limit).into(result);
		ins.close();
		return result;
	}

	public List<Document> listCollection(Bson sort, int from, int to) {
		List<Document> result = new ArrayList<Document>();
		if (from > to)
			return result;
		int limit = to - from + 1;
		if (from == 0)
			return listCollection(sort, limit);

		MongoDBInstance ins = getDBInstance();
		MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());

		collection.find().sort(sort).limit(limit).into(result);
		ins.close();
		return result;
	}

	/**
	 * 
	 * @param filter
	 * @return
	 */
	public List<Document> find(Bson filter) {
		MongoDBInstance ins = getDBInstance();
		MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
		List<Document> result = new ArrayList<Document>();
		collection.find(filter).into(result);
		ins.close();
		return result;
	}

	public List<Document> find(Bson filter, int limit) {
		MongoDBInstance ins = getDBInstance();
		MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
		List<Document> result = new ArrayList<Document>();
		collection.find(filter).limit(limit).into(result);
		ins.close();
		return result;
	}
	
	public List<Document> find(Bson filter, int from,int to) {
		List<Document> result = new ArrayList<Document>();
		if (from > to)
			return result;
		int limit = to - from + 1;
		if (from == 0)
			return find(filter,limit);
		
		MongoDBInstance ins = getDBInstance();
		MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
		
		collection.find(filter).limit(limit).skip(from).into(result);
		ins.close();
		return result;
	}

	public List<Document> find(Bson filter, Bson sort, int limit) {
		MongoDBInstance ins = getDBInstance();
		MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
		List<Document> result = new ArrayList<Document>();
		collection.find(filter).sort(sort).limit(limit).into(result);
		ins.close();
		return result;
	}
	
	public List<Document> find(Bson filter, Bson sort, int from,int to) {
		List<Document> result = new ArrayList<Document>();
		if (from > to)
			return result;
		int limit = to - from + 1;
		if (from == 0)
			return find(filter,sort,limit);
		
		MongoDBInstance ins = getDBInstance();
		MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
		result = new ArrayList<Document>();
		collection.find(filter).sort(sort).limit(limit).skip(from).into(result);
		ins.close();
		return result;
	}

	/**
	 * 
	 * @param filter
	 * @return
	 */
	public Document findOne(Bson filter) {
		MongoDBInstance ins = getDBInstance();
		MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
		Document d = collection.find(filter).first();
		ins.close();
		return d;
	}

	/**
	 * 
	 * @return
	 */
	public List<Object> list() {
		return _toList(listCollection());
	}

	/**
	 * 
	 * @param filter
	 * @return
	 */
	public Object findOneObject(Bson filter) {
		return MongoUtil.doc2Obj(findOne(filter));
	}

	/**
	 * 
	 * @param filter
	 * @return
	 */
	public List<Object> listByCondition(Bson filter) {
		return _toList(find(filter));
	}

	/**
	 * find one by id
	 * 
	 * @param id
	 * @return
	 */
	public Document get(String id) {
		return findOne(MongoUtil.getFilterByID(id));
	}
	
	public List<Document> getIDInList(List<String> ids){
		List<ObjectId> oidList = new ArrayList<ObjectId>();
		for (String id : ids){
			oidList.add(new ObjectId(id));
		}
		Document inQuery = new Document("$in",oidList);
		Document filter = new Document("_id",inQuery);
		return find(filter);
	}

	// --------------------------------------------------
	// insert funcitons
	// --------------------------------------------------
	/**
	 * @param list
	 * @return
	 */
	public boolean insert(List<Document> list) {
		String message = "";
		try {
			MongoDBInstance ins = getDBInstance();
			MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
			collection.insertMany(list);
			ins.close();
			message = "|insert SUCCESS | " + message;
			logMessage(message);
			return true;
		} catch (Exception e) {
			errorMessage(e.toString());
			message = "|insert FAIL | " + list.toString();
			errorMessage(message);
			return false;
		}
	}

	/**
	 * @param json
	 * @return
	 */
	public boolean insert(String json) {
		List<Document> list = MongoUtil.json2doclist(json);
		return insert(list);
	}

	/**
	 * @param document
	 * @return
	 */
	public String insertOne(Document document) {
		String message = "";
		try {
			MongoDBInstance ins = getDBInstance();
			MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
			collection.insertOne(document);
			ObjectId id = (ObjectId) document.get("_id");
			ins.close();
			message = "|insert SUCCESS | id:" + id + " | " + message;
			logMessage(message);
			return id.toString();
		} catch (Exception e) {
			errorMessage(e.toString());
			message = "|insert FAIL | " + document.toString();
			errorMessage(message);
			return null;
		}
	}

	/**
	 * @param json
	 * @return
	 */
	public String insertOne(String json) {
		Document document = MongoUtil.json2doc(json);
		return insertOne(document);
	}

	// --------------------------------------------------
	// update funcitons
	// --------------------------------------------------
	/**
	 * @param filter
	 * @param update
	 * @return
	 */
	public boolean update(Bson filter, Document update) {
		String message = "";
		try {
			MongoDBInstance ins = getDBInstance();
			MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
			UpdateResult result = collection.updateMany(filter, update);
			ins.close();
			message = "|update SUCCESS | " + result;
			logMessage(message);
			return true;
		} catch (Exception e) {
			errorMessage(e.toString());
			message = "|update FAIL | filter|" + filter.toString() + "|update|" + update.toString();
			errorMessage(message);
			return false;
		}
	}

	/**
	 * @param id
	 * @param update
	 * @return
	 */
	public boolean update(String id, Document update) {
		Bson filter = MongoUtil.getFilterByID(id);
		return update(filter, update);
	}

	/**
	 * @param filter
	 * @param update
	 * @return
	 */
	public boolean insertOrUpdate(Bson filter, Document update) {
		String message = "";
		try {
			UpdateOptions option = new UpdateOptions();
			option.upsert(true);
			MongoDBInstance ins = getDBInstance();
			MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
			UpdateResult result = collection.updateMany(filter, update, option);
			ins.close();
			message = "|update or insert SUCCESS | " + result.toString();
			logMessage(message);
			return true;
		} catch (Exception e) {
			errorMessage(e.toString());
			message = "|update or insert FAIL | filter|" + filter.toString() + "|update|" + update.toString();
			errorMessage(message);
			return false;
		}
	}

	/**
	 * 
	 * @param id
	 * @param update
	 * @return
	 */
	public boolean updateOne(String id, Document update) {
		String message = "";
		Bson filter = MongoUtil.getFilterByID(id);
		try {
			MongoDBInstance ins = getDBInstance();
			MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
			Document result = collection.findOneAndUpdate(filter, update);
			if (result == null) {
				message = "|update FAIL | filter|" + filter.toString() + "|update|" + update.toString();
				errorMessage(message);
				return false;
			}
			ins.close();
			message = "|update SUCCESS | " + result;
			logMessage(message);
			return true;
		} catch (Exception e) {
			errorMessage(e.toString());
			message = "|update FAIL | filter|" + filter.toString() + "|update|" + update.toString();
			errorMessage(message);
			return false;
		}
	}

	// --------------------------------------------------
	// delete funcitons
	// --------------------------------------------------
	/**
	 * @param filter
	 * @return
	 */
	public boolean delete(Bson filter) {
		String message = "";
		try {
			MongoDBInstance ins = getDBInstance();
			MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
			DeleteResult result = collection.deleteMany(filter);
			ins.close();
			message = "|delete SUCCESS | " + result.toString();
			logMessage(message);
			return true;
		} catch (Exception e) {
			errorMessage(e.toString());
			message = "|delete FAIL |" + filter.toString();
			errorMessage(message);
			return false;
		}
	}

	public boolean deleteOne(String id) {
		Bson filter = MongoUtil.getFilterByID(id);
		String message = "";
		try {
			MongoDBInstance ins = getDBInstance();
			MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
			Document result = collection.findOneAndDelete(filter);
			if (result == null) {
				message = "|delete FAIL |" + filter.toString();
				errorMessage(message);
				return false;
			}
			ins.close();
			message = "|delete SUCCESS | " + result.toString();
			logMessage(message);
			return true;
		} catch (Exception e) {
			errorMessage(e.toString());
			message = "|delete FAIL |" + filter.toString();
			errorMessage(message);
			return false;
		}
	}

	// none current collection related functions

	public boolean drop() {
		String message = "";
		try {
			MongoDBInstance ins = getDBInstance();
			MongoCollection<Document> collection = ins.getDB().getCollection(getCollectionName());
			collection.drop();
			ins.close();
			message = "|drop SUCCESS | ";
			logMessage(message);
			return true;
		} catch (Exception e) {
			errorMessage(e.toString());
			message = "|drop FAIL |";
			errorMessage(message);
			return false;
		}
	}

	public DBRef createDBRef(Document d) {
		DBRef ref = MongoUtil.createDBRefFromDocument(getCollectionName(), d);
		return ref;
	}

	public Document fetch(DBRef ref) {
		String collectionName = ref.getCollectionName();
		String id = (String) ref.getId();
		MongoDBInstance ins = getDBInstance();
		MongoCollection<Document> collection = ins.getDB().getCollection(collectionName);
		Bson filter = MongoUtil.getFilterByID(id);
		Document d = collection.find(filter).first();
		ins.close();
		return d;
	}

	public List<Document> reverseFetch(String field, DBRef ref) {
		Document filter = new Document(field, ref);
		return find(filter);
	}
	
	public List<Document> reverseFetchInlist(String field, List<DBRef>reflist) {
		Document inQuery = new Document("$in",reflist);
		Document filter = new Document(field, inQuery);
		return find(filter);
	}
	
	public List<Document> FindFieldInList(String field, List<Object> list){
		Document inQuery = new Document("$in",list);
		Document filter = new Document(field, inQuery);
		return find(filter);
	}
	
	public List<Document> reverseFetchInlist(String field, DBRef ref) {
		List<DBRef> list = new ArrayList<DBRef>();
		list.add(ref);
		return reverseFetchInlist(field,list);
	}
	
	public boolean deleteFieldOf(String field,Object fieldObj){
		Document filter = new Document(field,fieldObj);
		Document unset = new Document(field,null);
		Document update = new Document("$unset",unset);
		return update(filter, update);
	}
	
	
	/**
	 * 
	 * @param dA
	 * @param dB
	 * @param collectionAName
	 * @param fieldA
	 * @param collectionBName
	 * @param fieldB
	 * @return
	 */
	public boolean one2one(Document dA, Document dB, String collectionAName, String fieldA, String collectionBName,
			String fieldB) {
		String message = "";
		String idA = MongoUtil.getDocumentId(dA);
		String idB = MongoUtil.getDocumentId(dB);

		try {
			MongoDBInstance ins = getDBInstance();
			MongoCollection<Document> collectionA = ins.getDB().getCollection(collectionAName);
			MongoCollection<Document> collectionB = ins.getDB().getCollection(collectionBName);

			Bson filterA = MongoUtil.getFilterByID(idA);
			Bson filterB = MongoUtil.getFilterByID(idB);

			DBRef refA = new DBRef(collectionAName, idA);
			DBRef refB = new DBRef(collectionBName, idB);

			DBRef oldAx = (DBRef) dA.get(fieldA);
			DBRef oldBx = (DBRef) dB.get(fieldB);
			
			// A B linked
			if (oldAx != null && oldAx.getId().equals(idB) && oldBx != null && oldBx.getId().equals(idA)) {
				ins.close();
				message = "|link 1 to 1 existed | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
				logMessage(message);
				return true;
			}
			// unlink oldA
			if (oldAx != null){
				Document olddB = fetch(oldAx);
				unone2one(dA, olddB, fieldA, collectionBName, fieldB);
			}
			//unlink oldB
			
			if (oldBx != null){
				Document olddA = fetch(oldBx);
				unone2one(olddA, dB, fieldA, collectionBName, fieldB);
			}

			Document updateA = new Document("$set", new Document(fieldA, refB));
			Document updateB = new Document("$set", new Document(fieldB, refA));

			collectionA.findOneAndUpdate(filterA, updateA);
			collectionB.findOneAndUpdate(filterB, updateB);

			ins.close();
			message = "|link 1 to 1 SUCCESS | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
			logMessage(message);
			return true;
		} catch (Exception e) {
			errorMessage(e.toString());
			message = "|link 1 to 1 FAIL | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
			errorMessage(message);
			return false;
		}
	}
	
	/**
	 * 
	 * @param dA
	 * @param dB
	 * @param fieldA
	 * @param collectionBName
	 * @param fieldB
	 * @return get a result dA.fieldA = dB(DBRef), dB.filedB = dA(DBRef)
	 */
	public boolean one2one(Document dA, Document dB, String fieldA, String collectionBName, String fieldB) {
		return one2one(dA, dB, getCollectionName(), fieldA, collectionBName, fieldB);
	}

	@SuppressWarnings("unchecked")
	public boolean one2m(Document dA, Document dB, String collectionAName, String fieldA, String collectionBName,
			String fieldB) {
		String message = "";
		String idA = MongoUtil.getDocumentId(dA);
		String idB = MongoUtil.getDocumentId(dB);

		try {
			MongoDBInstance ins = getDBInstance();
			MongoCollection<Document> collectionA = ins.getDB().getCollection(collectionAName);
			MongoCollection<Document> collectionB = ins.getDB().getCollection(collectionBName);

			Bson filterA = MongoUtil.getFilterByID(idA);
			Bson filterB = MongoUtil.getFilterByID(idB);

			DBRef refA = new DBRef(collectionAName, idA);
			DBRef refB = new DBRef(collectionBName, idB);

			List<DBRef> bRef_list = (List<DBRef>) dA.get(fieldA);
			DBRef oldBx = (DBRef) dB.get(fieldB);
			
			// A B linked
			if (oldBx != null && oldBx.getId().equals(idA) && bRef_list != null && bRef_list.indexOf(refB) >-1) {
				ins.close();
				message = "|link 1 to m existed | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
				logMessage(message);
				return true;
			}
			// unlink oldB
			if (oldBx != null){
				Document olddA = fetch(oldBx);
				unone2m(olddA, dB, fieldA, collectionBName, fieldB);
			}
			//link A
			if (bRef_list == null || bRef_list.indexOf(refB) < 0){
				Document updateA = new Document("$addToSet", new Document(fieldA, refB));
				collectionA.findOneAndUpdate(filterA, updateA);
			}
			//link B
			Document updateB = new Document("$set", new Document(fieldB, refA));

			
			collectionB.findOneAndUpdate(filterB, updateB);

			ins.close();
			message = "|link 1 to m SUCCESS | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
			logMessage(message);
			return true;
		} catch (Exception e) {
			errorMessage(e.toString());
			message = "|link 1 to m FAIL | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
			errorMessage(message);
			return false;
		}
	}
	public boolean one2m(Document dA, Document dB, String fieldA, String collectionBName, String fieldB) {
		return one2m(dA, dB, getCollectionName(), fieldA, collectionBName, fieldB);
	}
	
	@SuppressWarnings("unchecked")
	public boolean m2m(Document dA, Document dB, String collectionAName, String fieldA, String collectionBName,
			String fieldB) {
		String message = "";
		String idA = MongoUtil.getDocumentId(dA);
		String idB = MongoUtil.getDocumentId(dB);

		try {
			MongoDBInstance ins = getDBInstance();
			MongoCollection<Document> collectionA = ins.getDB().getCollection(collectionAName);
			MongoCollection<Document> collectionB = ins.getDB().getCollection(collectionBName);

			Bson filterA = MongoUtil.getFilterByID(idA);
			Bson filterB = MongoUtil.getFilterByID(idB);

			DBRef refA = new DBRef(collectionAName, idA);
			DBRef refB = new DBRef(collectionBName, idB);

			List<DBRef> bRef_list = (List<DBRef>) dA.get(fieldA);
			List<DBRef> aRef_list = (List<DBRef>) dB.get(fieldA);
			// m to m existed
			if (bRef_list != null && bRef_list.indexOf(refB) > -1 && aRef_list != null && aRef_list.indexOf(refA) > -1){
				message = "|link m to m existed | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
				logMessage(message);
			}
			// link A
			if (bRef_list== null || bRef_list.indexOf(refB) < 0){
				Document updateA = new Document("$addToSet", new Document(fieldA, refB));
				collectionA.findOneAndUpdate(filterA, updateA);
			}
			// link B
			if (aRef_list== null || aRef_list.indexOf(refA) < 0){
				Document updateB = new Document("$addToSet", new Document(fieldB, refA));
				collectionB.findOneAndUpdate(filterB, updateB);
			}

			ins.close();
			message = "|link m to m SUCCESS | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
			logMessage(message);
			return true;
		} catch (Exception e) {
			errorMessage(e.toString());
			message = "|link m to m FAIL | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
			errorMessage(message);
			return false;
		}
	}
	public boolean m2m(Document dA, Document dB, String fieldA, String collectionBName, String fieldB) {
		return m2m(dA, dB, getCollectionName(), fieldA, collectionBName, fieldB);
	}
	
	public boolean unone2one(Document dA, Document dB, String collectionAName, String fieldA, String collectionBName,
			String fieldB) {
		String message = "";
		String idA = MongoUtil.getDocumentId(dA);
		String idB = MongoUtil.getDocumentId(dB);

		try {
			MongoDBInstance ins = getDBInstance();
			MongoCollection<Document> collectionA = ins.getDB().getCollection(collectionAName);
			MongoCollection<Document> collectionB = ins.getDB().getCollection(collectionBName);

			Bson filterA = MongoUtil.getFilterByID(idA);
			Bson filterB = MongoUtil.getFilterByID(idB);
			
			DBRef oldAx = (DBRef) dA.get(fieldA);
			DBRef oldBx = (DBRef) dB.get(fieldB);
			// already unlinked A B
			if (oldAx == null || oldBx == null || !oldAx.getId().equals(idB) || !oldBx.getId().equals(idB)){
				ins.close();
				message = "|unlink 1 to 1 not existed | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
				logMessage(message);
				return true;
			}
			// unlink A
	
			Document updateA = new Document("$unset", new Document(fieldA, null));
			collectionA.findOneAndUpdate(filterA, updateA);

			// unlink B
			Document updateB = new Document("$unset", new Document(fieldB, null));
			collectionB.findOneAndUpdate(filterB, updateB);

			
			ins.close();
			message = "|unlink 1 to 1 SUCCESS | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
			logMessage(message);
			return true;
		} catch (Exception e) {
			errorMessage(e.toString());
			message = "|unlink 1 to 1 FAIL | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
			errorMessage(message);
			return false;
		}
	}
	public boolean unone2one(Document dA, Document dB, String fieldA, String collectionBName, String fieldB) {
		return unone2one(dA, dB, getCollectionName(), fieldA, collectionBName, fieldB);
	}
	
	@SuppressWarnings("unchecked")
	public boolean unone2m(Document dA, Document dB, String collectionAName, String fieldA, String collectionBName,
			String fieldB) {
		String message = "";
		String idA = MongoUtil.getDocumentId(dA);
		String idB = MongoUtil.getDocumentId(dB);

		try {
			MongoDBInstance ins = getDBInstance();
			MongoCollection<Document> collectionA = ins.getDB().getCollection(collectionAName);
			MongoCollection<Document> collectionB = ins.getDB().getCollection(collectionBName);

			Bson filterA = MongoUtil.getFilterByID(idA);
			Bson filterB = MongoUtil.getFilterByID(idB);

			DBRef refB = new DBRef(collectionBName, idB);

			
			List<DBRef> bRef_list = (List<DBRef>) dA.get(fieldA);
			DBRef oldBx = (DBRef) dB.get(fieldB);
			logMessage(bRef_list.toString() + idB);
			logMessage(oldBx.toString() + idA);
			
			// A B not linked then dont need to un link
			if (oldBx == null || bRef_list == null || !oldBx.getId().equals(idA) || bRef_list.indexOf(refB) < 0) {
				ins.close();
				message = "|unlink 1 to m not existed | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
				logMessage(message);
				return true;
			}
			//un link A
			Document updateA = new Document("$pull", new Document(fieldA, refB));
			collectionA.findOneAndUpdate(filterA, updateA);
			//un link B
			Document updateB = new Document("$unset", new Document(fieldB, null));
			collectionB.findOneAndUpdate(filterB, updateB);

			ins.close();
			message = "|unlink 1 to m SUCCESS | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
			logMessage(message);
			return true;
		} catch (Exception e) {
			errorMessage(e.toString());
			message = "|unlink 1 to m FAIL | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
			errorMessage(message);
			return false;
		}
	}
	public boolean unone2m(Document dA, Document dB, String fieldA, String collectionBName, String fieldB) {
		return unone2m(dA, dB, getCollectionName(), fieldA, collectionBName, fieldB);
	}
	
	@SuppressWarnings("unchecked")
	public boolean unm2m(Document dA, Document dB, String collectionAName, String fieldA, String collectionBName,
			String fieldB) {
		String message = "";
		String idA = MongoUtil.getDocumentId(dA);
		String idB = MongoUtil.getDocumentId(dB);

		try {
			MongoDBInstance ins = getDBInstance();
			MongoCollection<Document> collectionA = ins.getDB().getCollection(collectionAName);
			MongoCollection<Document> collectionB = ins.getDB().getCollection(collectionBName);

			Bson filterA = MongoUtil.getFilterByID(idA);
			Bson filterB = MongoUtil.getFilterByID(idB);

			DBRef refA = new DBRef(collectionAName, idA);
			DBRef refB = new DBRef(collectionBName, idB);

			List<DBRef> bRef_list = (List<DBRef>) dA.get(fieldA);
			List<DBRef> aRef_list = (List<DBRef>) dB.get(fieldA);

			// un link A
			if (bRef_list != null && bRef_list.indexOf(refB) > -1){
				Document updateA = new Document("$pull", new Document(fieldA, refB));
				collectionA.findOneAndUpdate(filterA, updateA);
			}
			// un link B
			if (aRef_list != null && aRef_list.indexOf(refA) > -1){
				Document updateB = new Document("$pull", new Document(fieldB, refA));
				collectionB.findOneAndUpdate(filterB, updateB);
			}

			ins.close();
			message = "|unlink m to m SUCCESS | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
			logMessage(message);
			return true;
		} catch (Exception e) {
			errorMessage(e.toString());
			message = "|unlink m to m FAIL | " + collectionAName + ":" + idA + " | " + collectionBName + idB;
			errorMessage(message);
			return false;
		}
	}
	public boolean unm2m(Document dA, Document dB, String fieldA, String collectionBName, String fieldB) {
		return unm2m(dA, dB, getCollectionName(), fieldA, collectionBName, fieldB);
	}
	
//	public boolean link(Document dA)
}
