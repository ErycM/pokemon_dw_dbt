# {
#     version: 1.0,
#     name: Eryc Masselli, 22/08
#     description: PokeAPI extract and load for bigquery
# }

from pokebase import *
import numpy as np
import pandas as pd
from google.cloud import bigquery
import time
import os

def try_except(fn, excpt):
    try:
        return fn()
    except:
        return excpt


def main():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../data/pokemondw-e72d35150e00.json"
    client = bigquery.Client()

if __name__ == "__main__":
    main()