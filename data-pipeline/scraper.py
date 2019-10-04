import configparser
from elasticsearch import Elasticsearch
import requests as r
import json
from progressbar import ProgressBar
import time
import datetime

def requestData(requestURL, currentId):
	print("Querying for {}".format(currentId))
	try:
		response = r.get(requestURL)
		if(response.status_code==200):
			responseObject = response.json()
			if(responseObject is not None):
				if(responseObject[0]["commodity"] is not None):
					return responseObject
		else:
			return None
	except Exception as e:
		print("Exception caught in requestData at index # {}: {}".format(currentId, e))


def writeToES(es, parsedObject, dumpIndex, docType):
	try:
		esResponse = es.index(index=dumpIndex, doc_type=docType, body=parsedObject)

	except Exception as e:
		print("Experienced the Following Exception in writeToES: {}".format(e))


def main():
	config = configparser.ConfigParser()
	try:
		#load source settings
		config.read("settings.ini")
		startId = int(config["SOURCE"]["startId"])
		seedURL = config["SOURCE"]["seedURL"]
		endId = int(config["SOURCE"]["endId"])

		#load system settings
		currentId = int(config["DESTINATION"]["currentId"])
		dumpIndex = config["DESTINATION"]["dumpIndex"]
		docType = config["DESTINATION"]["docType"]
		writtenIds = {}
		items = {}

		with open('written.json') as json_file:
			writtenIds = json.load(json_file)

		if(currentId==-1):
			currentId = startId

		with open("items.json") as json_file:
			items = json.load(json_file)

		es = Elasticsearch()

		curr = currentId
		for i in range(curr, endId):
			if(str(i) not in writtenIds):
				requestURL = seedURL.replace("[sourceId]", str(i))
				response = requestData(requestURL, i)
				counts = 0
				if(response is not None):
					details = {}
					details["item"] = response[0]["commodity"]
					details["total"] = len(response)
					writtenIds[str(i)] = details
					with open('written.json', 'w') as outfile:
						json.dump(writtenIds, outfile)
					with open('items.json', 'w') as outfile:
						if(response[0]["commodity"] in items):
							items[response[0]["commodity"]] = items[response[0]["commodity"]]+len(response)
						else:
							items[response[0]["commodity"]] = len(response)

						json.dump(items, outfile)
					print("Total records of {} in {}: {}".format(response[0]["commodity"], i, len(response)))
					for j in range(len(response)):
						currentObj = response[j]
						dateField = currentObj["arrival_date"]
						dateArray = dateField.split("/")
						try:
							dateObj = {}
							dateObj["date"] = int(dateArray[0])
							dateObj["month"] = int(dateArray[1])
							dateObj["year"] = int(dateArray[2])
							currentObj["unix_time"] = int(time.mktime(datetime.datetime.strptime(dateField, "%d/%m/%Y").timetuple()))
							currentObj["arrival_date"] = dateObj
							currentObj["min_price"] = float(currentObj["min_price"])
							currentObj["max_price"] = float(currentObj["max_price"])
							currentObj["modal_price"] = float(currentObj["modal_price"])
							# print(currentObj)
							writeToES(es, currentObj, dumpIndex, docType)
							counts = counts+1
						except Exception as e:
							print("Skipping record # {} in sourceId # {}".format(j, i))
					print("Wrote {} of {} from {}".format(counts, response[0]["commodity"], i))
				else:
					print("Total records in {}: {}".format(i, 0))
					writtenIds[str(i)] = "None"
					with open('written.json', 'w') as outfile:
						json.dump(writtenIds, outfile)
					print("Wrote {} from {}".format(0, i))






	except Exception as e:
		print("Experienced the Following Exception in MAIN: {}".format(e))	


if __name__ == '__main__':
    main()
