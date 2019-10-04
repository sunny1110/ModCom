from elasticsearch import Elasticsearch

es = Elasticsearch()
index = "commodity6"
template = "sample4"


def getAggregateList(fieldMap, agg_field):
    query = prepareQuery(fieldMap, agg_field)
    results = launchESQuery(query)
    agg_count = results["aggregations"]["aggregate_count"]["value"]
    query["params"]["term_count"] = agg_count
    finalResults = launchESQuery(query)
    return finalResults["aggregations"]["aggregate_terms"]["buckets"]


def prepareCountQuery(fieldMap, agg_field):
    query = {
        "id": template,
        "params": fieldMap
    }
    query["params"]["aggregate"] = agg_field
    query["params"]["size"] = 0
    query["params"]["term_count"] = 1
    return query
        

def launchESQuery(query):
    res = es.search_template(index=index, body=query)
    return res