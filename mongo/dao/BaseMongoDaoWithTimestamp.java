package com.techstudio.dao;

import org.bson.Document;
import org.bson.conversions.Bson;

abstract public class BaseMongoDaoWithTimestamp extends BaseMongoDao {

	private static String CURRENTDATEKEY = "$currentDate";
	private static String LASTMODIFIED = "lastModified";
	private static String TYPEKEY = "$type";
	private static String TYPE = "timestamp";

	public BaseMongoDaoWithTimestamp(String _name) {
		super(_name);
	}

	private void _addLastModified(Document update) {
		Document curDate = (Document) update.get(CURRENTDATEKEY);
		if (curDate == null) {
			curDate = new Document();
			update.put(CURRENTDATEKEY, curDate);
		}
		curDate.put(LASTMODIFIED, new Document(TYPEKEY, TYPE));
	}

	// --------------------------------------------------
	// update funcitons
	// --------------------------------------------------
	/**
	 * @param filter
	 * @param update
	 * @return
	 */
	@Override
	public boolean update(Bson filter, Document update) {
		_addLastModified(update);
		return super.update(filter, update);
	}

	/**
	 * @param filter
	 * @param update
	 * @return
	 */
	@Override
	public boolean insertOrUpdate(Bson filter, Document update) {
		_addLastModified(update);
		return super.insertOrUpdate(filter, update);
	}
	
	/**
	 * 
	 * @param id
	 * @param update
	 * @return
	 */
	public boolean updateOne(String id,Document update){
		_addLastModified(update);
		return super.updateOne(id, update);
	}
}
