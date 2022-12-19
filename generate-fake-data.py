try:
    import uuid

    from datetime import datetime
    import os
    import logging
    import uuid
    import re
    from functools import wraps
    from enum import Enum
    from abc import ABC, abstractmethod
    from logging import StreamHandler

    from faker import Faker

    import pynamodb.attributes as at
    from pynamodb.models import Model
    from pynamodb.attributes import *
    from time import sleep
    from dotenv import load_dotenv
    load_dotenv(".env")
except Exception as e:
    raise Exception("Error : {}".format(e))


class DataGenerator(object):

    @staticmethod
    def get_data():
        faker = Faker()

        name = faker.name().split(" ")
        first_name = name[0]
        last_name = name[1]
        address = faker.address()
        text = faker.text()
        id = uuid.uuid4().__str__()
        city = faker.city()
        state = faker.state()

        _ = {
            "first_name": first_name,
            "last_name": last_name,
            "address": address,
            "text": text,
            "id": id,
            "city": city,
            "state": state
        }

        return _


class Users(Model):
    class Meta:
        table_name = os.getenv("DYNAMO_DB_TABLE_NAME")
        region = "us-east-1"
        aws_access_key_id =  os.getenv("DEV_ACCESS_KEY")
        aws_secret_access_key =  os.getenv("DEV_SECRET_KEY")

    pk = UnicodeAttribute(hash_key=True)
    sk= UnicodeAttribute(range_key=True)
    first_name = UnicodeAttribute()
    last_name = UnicodeAttribute()
    address = UnicodeAttribute()
    text = UnicodeAttribute()
    city = UnicodeAttribute()
    state = UnicodeAttribute()


def generate_data():
    for i in range(1, 500):
        json_data = DataGenerator.get_data()
        print(json_data)
        Users(
            pk=json_data.get("id").__str__(),
            sk=json_data.get("id").__str__(),
            first_name=json_data.get("first_name"),
            last_name=json_data.get("last_name"),
            address=json_data.get("address"),
            text=json_data.get("text"),
            city=json_data.get("address"),
            state=json_data.get("text")
        ).save()



generate_data()
