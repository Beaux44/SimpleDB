__all__ = ["QueryException", "assemble_query"]

class QueryException(Exception):
    pass



def assemble_query(query, *, _toplevel=True):
    predicate_dict = {}

    try:
        for k, v in query.items():
            if k.startswith("$"):
                predicate_dict[k] = commands[k](v)
            elif isinstance(v, dict):
                predicate_dict[k] = assemble_query(v, _toplevel=False)
            else:
                predicate_dict[k] = com_eq(v)

    except QueryException as e:
        if _toplevel:
            raise QueryException(f"Failed to parse query {query!r} in sub-query {e}")
        else:
            raise e


    if len(predicate_dict) == 1 and [*predicate_dict.keys()][0].startswith("$"):
        return [*predicate_dict.values()][0]
    else:
        return com_and(predicate_dict)


commands = {}

def command(name):
    def func(cb):
        commands["$" + name] = cb
        return cb

    return func


@command("in")
def com_in(query):
    def pred(val) -> bool:
        return val in query

    return pred

@command("eq")
def com_eq(query):
    def pred(val) -> bool:
        return val == query

    return pred

@command("lt")
def com_lt(query):
    def pred(val) -> bool:
        return val < query

    return pred

@command("gt")
def com_gt(query):
    def pred(val) -> bool:
        return val > query

    return pred

@command("or")
def com_or(query):
    def pred(val) -> bool:
        for key, pred in query.items():
            test = val.get(key)
            if test is not None and pred(test):
                return True

        return False
    return pred

@command("and")
def com_and(query):
    def pred(val) -> bool:
        if isinstance(val, dict):
            try:
                return all(pred(val[key]) for key, pred in query.items())
            except KeyError:
                return False

        return all(pred(val) for pred in query.values())

    return pred



if __name__ == "__main__":
    qry = assemble_query({"team": "A", "score": {"$gt": 22, "$lt": 24}})
    print(qry)
    print(qry({"team": "A", "score": 22}))
    print(qry({"team": "A", "score": 23}))
    print(qry({"team": "A", "score": 24}))
    print(qry({"team": "B", "score": 15}))
    print(qry({"team": "Q", "score": 65}))
