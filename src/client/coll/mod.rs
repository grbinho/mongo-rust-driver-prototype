mod batch;
pub mod error;
pub mod options;
pub mod results;

use bson::{self, Bson};

use self::batch::Batch;
use self::options::*;
use self::results::*;

use client::db::Database;
use client::common::{ReadPreference, WriteConcern};

use client::cursor::Cursor;
use client::Result;

use client::oid;

use client::coll::error::{BulkWriteException, WriteException};
use client::Error::{ArgumentError, ResponseError,
                    OperationError, BulkWriteError, WriteError};

use client::wire_protocol::flags::OpQueryFlags;

use std::collections::BTreeMap;

/// Interfaces with a MongoDB collection.
pub struct Collection<'a> {
    pub db: &'a Database<'a>,
    pub namespace: String,
    read_preference: ReadPreference,
    write_concern: WriteConcern,
}

impl<'a> Collection<'a> {
    /// Creates a collection representation with optional read and write controls.
    ///
    /// If `create` is specified, the collection will be explicitly created in the database.
    pub fn new(db: &'a Database<'a>, name: &str, create: bool,
               read_preference: Option<ReadPreference>, write_concern: Option<WriteConcern>) -> Collection<'a> {

        let rp = read_preference.unwrap_or(db.read_preference.to_owned());
        let wc = write_concern.unwrap_or(db.write_concern.to_owned());

        Collection {
            db: db,
            namespace: format!("{}.{}", db.name, name),
            read_preference: rp,
            write_concern: wc,
        }
    }

    /// Returns a unique operational request id.
    pub fn get_req_id(&self) -> i32 {
        self.db.client.get_req_id()
    }

    /// Extracts the collection name from the namespace.
    /// If the namespace is invalid, this method will panic.
    pub fn name(&self) -> String {
        match self.namespace.find(".") {
            Some(idx) => self.namespace[self.namespace.char_indices()
                                        .nth(idx+1).unwrap().0..].to_owned(),
            None => {
                // '.' is inserted in Collection::new, so this should only panic due to user error.
                let msg = format!("Invalid namespace specified: '{}'.", self.namespace);
                panic!(msg);
            }
        }
    }

    /// Permanently deletes the collection from the database.
    pub fn drop(&'a self) -> Result<()> {
        self.db.drop_collection(&self.name()[..])
    }

    /// Runs an aggregation framework pipeline.
    pub fn aggregate(&'a self, pipeline: Vec<bson::Document>, options: Option<AggregateOptions>) -> Result<Cursor<'a>> {
        let opts = options.unwrap_or(AggregateOptions::new());

        let pipeline_map = pipeline.iter().map(|bdoc| {
            Bson::Document(bdoc.to_owned())
        }).collect();

        let mut spec = bson::Document::new();
        let mut cursor = bson::Document::new();
        cursor.insert("batchSize".to_owned(), Bson::I32(opts.batch_size));
        spec.insert("aggregate".to_owned(), Bson::String(self.name()));
        spec.insert("pipeline".to_owned(), Bson::Array(pipeline_map));
        spec.insert("cursor".to_owned(), Bson::Document(cursor));
        if opts.allow_disk_use {
            spec.insert("allowDiskUse".to_owned(), Bson::Boolean(opts.allow_disk_use));
        }

        self.db.command_cursor(spec)
    }

    /// Gets the number of documents matching the filter.
    pub fn count(&self, filter: Option<bson::Document>, options: Option<CountOptions>) -> Result<i64> {
        let opts = options.unwrap_or(CountOptions::new());

        let mut spec = bson::Document::new();
        spec.insert("count".to_owned(), Bson::String(self.name()));
        spec.insert("skip".to_owned(), Bson::I64(opts.skip as i64));
        spec.insert("limit".to_owned(), Bson::I64(opts.limit));
        if filter.is_some() {
            spec.insert("query".to_owned(), Bson::Document(filter.unwrap()));
        }

        // Favor specified hint document over string
        if opts.hint_doc.is_some() {
            spec.insert("hint".to_owned(), Bson::Document(opts.hint_doc.unwrap()));
        } else if opts.hint.is_some() {
            spec.insert("hint".to_owned(), Bson::String(opts.hint.unwrap()));
        }

        let result = try!(self.db.command(spec));
        match result.get("n") {
            Some(&Bson::I32(ref n)) => Ok(*n as i64),
            Some(&Bson::I64(ref n)) => Ok(*n),
            _ => Err(ResponseError("No count received from server.".to_owned())),
        }
    }

    /// Finds the distinct values for a specified field across a single collection.
    pub fn distinct(&self, field_name: &str, filter: Option<bson::Document>, options: Option<DistinctOptions>) -> Result<Vec<Bson>> {

        let opts = options.unwrap_or(DistinctOptions::new());

        let mut spec = bson::Document::new();
        spec.insert("distinct".to_owned(), Bson::String(self.name()));
        spec.insert("key".to_owned(), Bson::String(field_name.to_owned()));
        if filter.is_some() {
            spec.insert("query".to_owned(), Bson::Document(filter.unwrap()));
        }

        let result = try!(self.db.command(spec));
        match result.get("values") {
            Some(&Bson::Array(ref vals)) => Ok(vals.to_owned()),
            _ => Err(ResponseError("No values received from server.".to_owned()))
        }
    }

    /// Returns a list of documents within the collection that match the filter.
    pub fn find(&self, filter: Option<bson::Document>, options: Option<FindOptions>)
                -> Result<Cursor<'a>> {

        let doc = filter.unwrap_or(bson::Document::new());
        let options = options.unwrap_or(FindOptions::new());
        let flags = OpQueryFlags::with_find_options(&options);

        Cursor::query_with_batch_size(&self.db.client, self.namespace.to_owned(),
                                      options.batch_size, flags, options.skip as i32,
                                      options.limit, doc, options.projection.clone(),
                                      false)
    }

    /// Returns the first document within the collection that matches the filter, or None.
    pub fn find_one(&self, filter: Option<bson::Document>, options: Option<FindOptions>)
                    -> Result<Option<bson::Document>> {
        let options = options.unwrap_or(FindOptions::new());
        let mut cursor = try!(self.find(filter, Some(options.with_limit(1))));
        match cursor.next() {
            Some(Ok(bson)) => Ok(Some(bson)),
            Some(Err(err)) => Err(err),
            None => Ok(None)
        }
    }

    // Helper method for all findAndModify commands.
    fn find_and_modify(&self, cmd: &mut bson::Document,
                       filter: bson::Document, max_time_ms: Option<i64>,
                       projection: Option<bson::Document>, sort: Option<bson::Document>,
                       write_concern: Option<WriteConcern>)
                       -> Result<Option<bson::Document>> {

        let wc = write_concern.unwrap_or(self.write_concern.clone());

        let mut new_cmd = bson::Document::new();
        new_cmd.insert("findAndModify".to_owned(), Bson::String(self.name()));
        new_cmd.insert("query".to_owned(), Bson::Document(filter));
        new_cmd.insert("writeConcern".to_owned(), Bson::Document(wc.to_bson()));
        if sort.is_some() {
            new_cmd.insert("sort".to_owned(), Bson::Document(sort.unwrap()));
        }
        if projection.is_some() {
            new_cmd.insert("fields".to_owned(), Bson::Document(projection.unwrap()));
        }

        for (key, val) in cmd.iter() {
            new_cmd.insert(key.to_owned(), val.to_owned());
        }

        let res = try!(self.db.command(new_cmd));
        try!(WriteException::validate_write_result(res.clone(), wc));
        let doc = match res.get("value") {
            Some(&Bson::Document(ref nested_doc)) => Some(nested_doc.to_owned()),
            _ => None,
        };

        Ok(doc)
    }

    // Helper method for validated replace and update commands.
    fn find_one_and_replace_or_update(&self, filter: bson::Document, update: bson::Document,
                                      after: bool, max_time_ms: Option<i64>,
                                      projection: Option<bson::Document>, sort: Option<bson::Document>,
                                      upsert: bool, write_concern: Option<WriteConcern>) -> Result<Option<bson::Document>> {

        let mut cmd = bson::Document::new();
        cmd.insert("update".to_owned(), Bson::Document(update));
        if after {
            cmd.insert("new".to_owned(), Bson::Boolean(true));
        }
        if upsert {
            cmd.insert("upsert".to_owned(), Bson::Boolean(true));
        }

        self.find_and_modify(&mut cmd, filter, max_time_ms, projection, sort, write_concern)
    }

    /// Finds a single document and deletes it, returning the original.
    pub fn find_one_and_delete(&self, filter: bson::Document,
                               options: Option<FindOneAndDeleteOptions>)  -> Result<Option<bson::Document>> {

        let opts = options.unwrap_or(FindOneAndDeleteOptions::new());
        let mut cmd = bson::Document::new();
        cmd.insert("remove".to_owned(), Bson::Boolean(true));
        self.find_and_modify(&mut cmd, filter, opts.max_time_ms,
                             opts.projection, opts.sort, opts.write_concern)
    }

    /// Finds a single document and replaces it, returning either the original
    /// or replaced document.
    pub fn find_one_and_replace(&self, filter: bson::Document, replacement: bson::Document,
                                options: Option<FindOneAndUpdateOptions>)  -> Result<Option<bson::Document>> {
        let opts = options.unwrap_or(FindOneAndUpdateOptions::new());
        try!(Collection::validate_replace(&replacement));
        self.find_one_and_replace_or_update(filter, replacement, opts.return_document.to_bool(),
                                            opts.max_time_ms, opts.projection, opts.sort,
                                            opts.upsert, opts.write_concern)
    }

    /// Finds a single document and updates it, returning either the original
    /// or updated document.
    pub fn find_one_and_update(&self, filter: bson::Document, update: bson::Document,
                               options: Option<FindOneAndUpdateOptions>)  -> Result<Option<bson::Document>> {
        let opts = options.unwrap_or(FindOneAndUpdateOptions::new());
        try!(Collection::validate_update(&update));
        self.find_one_and_replace_or_update(filter, update, opts.return_document.to_bool(),
                                            opts.max_time_ms, opts.projection, opts.sort,
                                            opts.upsert, opts.write_concern)
    }

    pub fn get_unordered_batches(requests: Vec<WriteModel>) -> Vec<Batch> {
        let mut inserts = vec![];

        for req in requests {
            match req {
                WriteModel::InsertOne { document }  => {
                    inserts.push(document)
                }
                _ => ()
            }
        }

        vec![Batch::Insert { documents: inserts }]
    }

    pub fn get_ordered_batches(requests: Vec<WriteModel>) -> Vec<Batch> {
        let mut inserts = vec![];

        for req in requests {
            match req {
                WriteModel::InsertOne { document }  => {
                    inserts.push(document)
                }
                _ => ()
            }
        }

        vec![Batch::Insert { documents: inserts }]
    }

    fn execute_insert_one_batch(&self, document: bson::Document, i: i64,
                                result: &mut BulkWriteResult,
                                exception: &mut BulkWriteException) {
        let model = WriteModel::InsertOne { document: document.clone() };

        match self.insert_one(document, None) {
            Ok(insert_result) => {

                result.process_insert_one_result(insert_result, i, model,
                                                 exception);
            },
            Err(err) => exception.add_unproccessed_model(model)
        }
    }

    fn execute_insert_many_batch(&self, documents: Vec<bson::Document>,
                                 ordered: bool, result: &mut BulkWriteResult,
                                 exception: &mut BulkWriteException) {
        let models = documents.iter().map(|doc|
          WriteModel::InsertOne { document: doc.clone() }
        ).collect();

        match self.insert_many(documents, ordered, None) {
            Ok(insert_result) =>
                result.process_insert_many_result(insert_result, models,
                                                  exception),
            Err(err) => exception.add_unproccessed_models(models)
        }
    }

    fn execute_batch(&self, batch: Batch, ordered: bool, i: i64,
                     result: &mut BulkWriteResult,
                     exception: &mut BulkWriteException) {
        match batch {
            Batch::Insert { mut documents } =>
                if documents.len() == 1 {
                    self.execute_insert_one_batch(documents.pop().unwrap(), i, result,
                                                  exception)
                } else {
                    self.execute_insert_many_batch(documents, ordered,
                                                       result, exception)
                }
        }
    }

    /// Sends a batch of writes to the server at the same time.
    pub fn bulk_write(&self, requests: Vec<WriteModel>, ordered: bool) -> BulkWriteResult {
        let batches = if ordered {
                          Collection::get_ordered_batches(requests)
                      } else {
                          Collection::get_unordered_batches(requests)
                      };

        let mut result = BulkWriteResult::new();
        let mut exception = BulkWriteException::new(vec![], vec![], vec![], None);

        for (i, batch) in batches.into_iter().enumerate() {
            self.execute_batch(batch, ordered, i as i64, &mut result,
                               &mut exception);
        }

        if exception.unprocessed_requests.len() == 0 {
            result.bulk_write_exception = Some(exception);
        }

        result
    }

    // Internal insertion helper function. Returns a vec of collected ids and a possible exception.
    fn insert(&self, docs: Vec<bson::Document>, ordered: bool,
              write_concern: Option<WriteConcern>) -> Result<(Vec<Bson>,
                                                              Option<BulkWriteException>)> {

        let wc =  write_concern.unwrap_or(self.write_concern.clone());

        let mut converted_docs = Vec::new();
        let mut ids = Vec::new();

        for doc in &docs {
            let mut cdoc = doc.to_owned();
            match doc.get("_id") {
                Some(id) => ids.push(id.clone()),
                None => {
                    let id = Bson::ObjectId(try!(oid::ObjectId::new()).bytes());
                    cdoc.insert("_id".to_owned(), id.clone());
                    ids.push(id);
                }
            }
            converted_docs.push(Bson::Document(cdoc));
        }

        let mut cmd = bson::Document::new();
        cmd.insert("insert".to_owned(), Bson::String(self.name()));
        cmd.insert("documents".to_owned(), Bson::Array(converted_docs));
        cmd.insert("ordered".to_owned(), Bson::Boolean(ordered));
        cmd.insert("writeConcern".to_owned(), Bson::Document(wc.to_bson()));

        let result = try!(self.db.command(cmd));

        // Intercept bulk write exceptions and insert into the result
        let exception_res = BulkWriteException::validate_bulk_write_result(result.clone(), wc);
        let exception = match exception_res {
            Ok(()) => None,
            Err(BulkWriteError(err)) => Some(err),
            Err(e) => return Err(e),
        };

        Ok((ids, exception))
    }

    /// Inserts the provided document. If the document is missing an identifier,
    /// the driver should generate one.
    pub fn insert_one(&self, doc: bson::Document, write_concern: Option<WriteConcern>) -> Result<InsertOneResult> {
        let (ids, bulk_exception) = try!(self.insert(vec!(doc), true, write_concern.clone()));

        if ids.len() == 0 {
            return Err(OperationError("No ids returned for insert_one.".to_owned()));
        }

        // Downgrade bulk exception, if it exists.
        let exception = match bulk_exception {
            Some(e) => Some(WriteException::with_bulk_exception(e)),
            None => None,
        };

        let id = match exception {
            Some(ref exc) => match exc.write_error {
                Some(_) => None,
                None => Some(ids[0].to_owned()),
            },
            None => Some(ids[0].to_owned()),
        };

        Ok(InsertOneResult::new(id, exception))
    }

    /// Inserts the provided documents. If any documents are missing an identifier,
    /// the driver should generate them.
    pub fn insert_many(&self, docs: Vec<bson::Document>, ordered: bool,
                       write_concern: Option<WriteConcern>) -> Result<InsertManyResult> {

        let (ids, exception) = try!(self.insert(docs, ordered, write_concern));

        let mut map = BTreeMap::new();
        for i in 0..ids.len() {
            map.insert(i as i64, ids.get(i).unwrap().to_owned());
        }

        if let Some(ref exc) = exception {
            for error in &exc.write_errors {
                map.remove(&(error.index as i64));
            }
        }

        Ok(InsertManyResult::new(Some(map), exception))
    }

    // Internal deletion helper function.
    fn delete(&self, filter: bson::Document, limit: i64, write_concern: Option<WriteConcern>) -> Result<DeleteResult> {
        let wc = write_concern.unwrap_or(self.write_concern.clone());

        let mut deletes = bson::Document::new();
        deletes.insert("q".to_owned(), Bson::Document(filter));
        deletes.insert("limit".to_owned(), Bson::I64(limit));

        let mut cmd = bson::Document::new();
        cmd.insert("delete".to_owned(), Bson::String(self.name()));
        cmd.insert("deletes".to_owned(), Bson::Array(vec!(Bson::Document(deletes))));
        cmd.insert("writeConcern".to_owned(), Bson::Document(wc.to_bson()));

        let result = try!(self.db.command(cmd));

        // Intercept write exceptions and insert into the result
        let exception_res = WriteException::validate_write_result(result.clone(), wc);
        let exception = match exception_res {
            Ok(()) => None,
            Err(WriteError(err)) => Some(err),
            Err(e) => return Err(e),
        };

        Ok(DeleteResult::new(result, exception))
    }

    /// Deletes a single document.
    pub fn delete_one(&self, filter: bson::Document, write_concern: Option<WriteConcern>) -> Result<DeleteResult> {
        self.delete(filter, 1, write_concern)
    }

    /// Deletes multiple documents.
    pub fn delete_many(&self, filter: bson::Document, write_concern: Option<WriteConcern>) -> Result<DeleteResult> {
        self.delete(filter, 0, write_concern)
    }

    // Internal update helper function.
    fn update(&self, filter: bson::Document, update: bson::Document, upsert: bool, multi: bool,
              write_concern: Option<WriteConcern>) -> Result<UpdateResult> {

        let wc = write_concern.unwrap_or(self.write_concern.clone());

        let mut updates = bson::Document::new();
        updates.insert("q".to_owned(), Bson::Document(filter));
        updates.insert("u".to_owned(), Bson::Document(update));
        updates.insert("upsert".to_owned(), Bson::Boolean(upsert));
        if multi {
            updates.insert("multi".to_owned(), Bson::Boolean(multi));
        }

        let mut cmd = bson::Document::new();
        cmd.insert("update".to_owned(), Bson::String(self.name()));
        cmd.insert("updates".to_owned(), Bson::Array(vec!(Bson::Document(updates))));
        cmd.insert("writeConcern".to_owned(), Bson::Document(wc.to_bson()));

        let result = try!(self.db.command(cmd));

        // Intercept write exceptions and insert into the result
        let exception_res = WriteException::validate_write_result(result.clone(), wc);
        let exception = match exception_res {
            Ok(()) => None,
            Err(WriteError(err)) => Some(err),
            Err(e) => return Err(e),
        };

        Ok(UpdateResult::new(result, exception))
    }

    /// Replaces a single document.
    pub fn replace_one(&self, filter: bson::Document, replacement: bson::Document, upsert: bool,
                       write_concern: Option<WriteConcern>) -> Result<UpdateResult> {

        let _ = try!(Collection::validate_replace(&replacement));
        self.update(filter, replacement, upsert, false, write_concern)
    }

    /// Updates a single document.
    pub fn update_one(&self, filter: bson::Document, update: bson::Document, upsert: bool,
                      write_concern: Option<WriteConcern>) -> Result<UpdateResult> {

        let _ = try!(Collection::validate_update(&update));
        self.update(filter, update, upsert, false, write_concern)
    }

    /// Updates multiple documents.
    pub fn update_many(&self, filter: bson::Document, update: bson::Document, upsert: bool,
                       write_concern: Option<WriteConcern>) -> Result<UpdateResult> {

        let _ = try!(Collection::validate_update(&update));
        self.update(filter, update, upsert, true, write_concern)
    }

    fn validate_replace(replacement: &bson::Document) -> Result<()> {
        for key in replacement.keys() {
            if key.starts_with("$") {
                return Err(ArgumentError("Replacement cannot include $ operators.".to_owned()));
            }
        }
        Ok(())
    }

    fn validate_update(update: &bson::Document) -> Result<()> {
        for key in update.keys() {
            if !key.starts_with("$") {
                return Err(ArgumentError("Update only works with $ operators.".to_owned()));
            }
        }
        Ok(())
    }
}