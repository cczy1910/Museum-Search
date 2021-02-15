import json
import math
import random
from collections import defaultdict
from zipfile import ZipFile

import pymongo


class Service:
    """
    Basic class for database queries
    :param reload_data: reloads data if True
    :param indexes: indexer to create if data is reloaded
    :param collection: collection to work with
    """

    def __init__(self, reload_data, indexes, collection):
        self.collection = collection
        if reload_data:
            self.collection.drop()
            for index in indexes:
                self.collection.create_index(list(index))


class MuseumsService(Service):
    """
    Class for museums search
    "Госкаталог Музеи и Галереи" is used as dataset for search
    :param reload_data: reloads data if True
    :param file-name: file with data
    """

    def __init__(self, reload_data=False, file_name="data-19-structure-4.json"):
        collection = pymongo.MongoClient().client.gc.museums
        indexes = [[('data.general.externalids.statistic', pymongo.ASCENDING)]]
        super().__init__(reload_data, indexes, collection)
        if reload_data:
            self.__load_data(file_name)

    def __load_data(self, file):
        with open(file) as myfile:
            data = json.loads(myfile.read())
            for d in data:
                self.collection.insert_one(d)

    def get_by_id(self, id):
        """
        Finds the museum by  id
        :param id: "KOPUK"-id of museum
        """
        return self.collection.find_one({
            'data.general.externalIds.statistic': int(id)
        })


class ExhibitService(Service):
    """
    Class for exhibits search
    "Госкаталог Музейного фонда Российской Федерации" is used as dataset for search
    :param reload_data: reloads data if True
    :param file-name: file with data
    :param represent: part of dataset to load
    """

    def __init__(self, reload_data=False, file_name="data-4-structure-3.json.zip", represent=0.1):
        collection = pymongo.MongoClient().client.gc.subj
        indexes = [[('data.authors', pymongo.TEXT), ('data.name', pymongo.TEXT), ('data.description', pymongo.TEXT)]]
        super().__init__(reload_data, indexes, collection)
        if reload_data:
            self.__load_data(file_name, represent)

    def __load_data(self, file, represent):
        with ZipFile(file) as myzip:
            for name in myzip.namelist():
                with myzip.open(name) as myfile:
                    print(name)
                    data = json.loads(myfile.read().decode('utf8'))
                    for d in data:
                        if random.random() < represent:
                            self.collection.insert_one(d)

    def get_by_name(self, query):
        """
        Find all the exhibits which contains specified word in their description.
        :param query: search query
        """
        return self.collection.find({
            '$text': {
                '$search': query
            }
        })


class Navigator:
    """
    Provides interface for search queries
    """

    def __init__(self):
        self.museums_service = MuseumsService()
        self.exhibit_service = ExhibitService()

    def __get_museums_with_locales_and_exhibits(self, query):
        """
        Search museums by query
        :param query: search query
        :return: dictionary containing information about museums, their location and exhibits
        """
        museums = {}
        exhibits = list(self.exhibit_service.get_by_name(query))
        for exhibit in exhibits:
            museum_id = exhibit['data']['museum']['code']
            exhibit_name = exhibit['data']['name']
            if museum_id is not None and exhibit_name is not None:
                if museum_id in museums.keys():
                    museums[museum_id]['exhibits'].append(exhibit_name)
                else:
                    museum = self.museums_service.get_by_id(museum_id)
                    if museum is not None:
                        locale = museum['data']['general']['locale']['name']
                        query = museum['data']['general']['name']
                        if locale is not None and query is not None:
                            museums[museum_id] = {}
                            museums[museum_id]['name'] = query
                            museums[museum_id]['locale'] = locale
                            museums[museum_id]['exhibits'] = [exhibit_name]
        return museums

    @staticmethod
    def __filter_relevant_museums(museums, border=0.04, select=0.8):
        """
        Filter response by relevancy
        :param museums: response content
        :param border: lower bound for number of relevant exhibits
        :param select: part of found museums to return
        :return: filtered response
        """
        res_museums = list(reversed(sorted(museums.values(), key=lambda m: len(m['exhibits']))))
        max_exhibits = len(res_museums[0]['exhibits'])
        res_museums = list(filter(lambda m: (len(m['exhibits']) / max_exhibits) >= border, res_museums))
        return res_museums[:math.ceil(len(res_museums) * select)]

    def get_sorted_locales(self, query, limit=7):
        """
        Find locations and museums in them
        :param query: search query
        :param limit: max number distinct location
        :return: response content
        """
        locales = defaultdict(list)
        museums = self.__filter_relevant_museums(self.__get_museums_with_locales_and_exhibits(query))
        for museum in museums:
            locales[museum['locale']].append(museum)
        sorted_locales = list(reversed(sorted(list(locales.keys()), key=lambda l: len(locales[l]))))
        sorted_locales = sorted_locales[:min(limit, len(sorted_locales))]
        return list(map(lambda l: (l, locales[l]), sorted_locales))

    def print_locales(self, query):
        """
        Print locations and museums in them
        :param query: search query
        """
        locales = self.get_sorted_locales(query)
        print("Найдены музеи в " + str(len(locales)) + " городах!")
        for locale in locales:
            if len(locale[1]) == 1:
                print(locale[0] + " - 1 музей:")
            else:
                print(locale[0] + " - " + str(len(locale[1])) + " музеев:")
            for museum in locale[1]:
                print("  " + museum['name'])
                for exhibit in museum['exhibits'][:min(5, len(museum['exhibits']))]:
                    print("    " + exhibit)
