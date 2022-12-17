class InsertOneResult(object):
    def __init__(self, inserted_id):
        self.acknowledged = True
        self.inserted_id = inserted_id


class InsertManyResult(object):
    def __init__(self, inserted_ids):
        self.acknowledged = True
        self.inserted_ids = inserted_ids


class UpdateResult(object):
    def __init__(self, matched_count, modified_count, upserted_id=None):
        self.acknowledged = True
        self.matched_count = matched_count
        self.modified_count = modified_count
        self.upserted_id = upserted_id


class DeleteResult(object):
    def __init__(self, deleted_count):
        self.acknowledged = True
        self.deleted_count = deleted_count


class BulkWriteResult(object):
    def __init__(self, deleted_count, inserted_count, matched_count, modified_count, upserted_count, upserted_ids):
        self.deleted_count = deleted_count
        self.inserted_count = inserted_count
        self.matched_count = matched_count
        self.modified_count = modified_count
        self.upserted_count = upserted_count
        self.upserted_ids = upserted_ids



