# coding: utf-8

"""
	Ce script a pour objectif de d'effectuer le chargement d'un fichier Excel selon différentes méthodes.
	Crédits : https://www.kdnuggets.com/2021/09/excel-files-python-1000x-faster-way.html
"""

from __future__ import annotations
from threading import Lock, Thread
from typing import Optional

import os, logging
import pandas as pd
import numpy as np
from joblib import Parallel, delayed
import time

class ExcelFileReaderMeta(type): # Définition de la classe ExcelFileReader
	"""Classe permettant d'implémenter un Singleton thread-safe"""

	_instance: Optional[ExcelFileReader] = None

	_lock: Lock = Lock()
	"""On a posé un verrou sur cet objet. Il sera utilisé pour la synchronisation des threads lors du premier accès au Singleton."""

	def __call__(cls, *args, **kwargs):
		# Now, imagine that the program has just been launched. Since there's no
		# Singleton instance yet, multiple threads can simultaneously pass the
		# previous conditional and reach this point almost at the same time. The
		# first of them will acquire lock and will proceed further, while the
		# rest will wait here.
		with cls._lock:
			# The first thread to acquire the lock, reaches this conditional,
			# goes inside and creates the Singleton instance. Once it leaves the
			# lock block, a thread that might have been waiting for the lock
			# release may then enter this section. But since the Singleton field
			# is already initialized, the thread won't create a new object.
			if not cls._instance:
				cls._instance = super().__call__(*args, **kwargs)
		return cls._instance

class ExcelFileReader(metaclass=ExcelFileReaderMeta): # Définition de la classe ExcelFileReader
	"""Classe définissant ExcelFileReader, qui permettra de lire le contenu d'un fichier Excel"""

	def __init__(self): # Constructeur
		self.logger = None

		# On configure le logger
		LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
		logging.basicConfig(level=logging.DEBUG, filemode='w', format=LOG_FORMAT)
		formatter = logging.Formatter(LOG_FORMAT)
		self.logger = logging.getLogger('excelfilereader')

		# Les logs seront inscrits dans un fichier
		fileHandler = logging.FileHandler("../log/excelfilereader.log", mode='w')
		fileHandler.setFormatter(formatter)
		self.logger.addHandler(fileHandler)
		self.logger.info("Logger initialization complete ...")

	def init(self):
		"""Initialise les fichiers nécessaires pour effectuer ce test de lecture"""

		self.logger.info("Initializing dummy files ...")

		# On génère 10 fichiers de 20000 lignes sur 25 colonnes avec des valeurs aléatoires
		for file_number in range(10):
			values = np.random.uniform(size=(20000,25))

			self.logger.debug("Initializing CSV dummy file %s", file_number)
			pd.DataFrame(values).to_csv(f"../in/Dummy {file_number}.csv")

			self.logger.debug("Initializing Excel dummy file %s", file_number)
			pd.DataFrame(values).to_excel(f"../in/Dummy {file_number}.xlsx")

			self.logger.debug("Initializing Pickle dummy file %s", file_number)
			pd.DataFrame(values).to_pickle(f"../in/Dummy {file_number}.pickle")		

	def loadExcelFiles(self):
		"""On charge les fichiers Excel générés"""

		self.logger.info("Start of loading the generated Excel files")

		start = time.time()
		df = pd.read_excel("../in/Dummy 0.xlsx")
		for file_number in range(1,10):
			self.logger.debug("Loading Excel dummy file %s", file_number)
			df.append(pd.read_excel(f"../in/Dummy {file_number}.xlsx"))
		end = time.time()

		self.logger.info("End of loading the generated Excel files : %s", end - start)

	def loadCSVFiles(self):
		"""On charge les fichiers CSV générés"""

		self.logger.info("Start of loading the generated CSV files")

		start = time.time()
		df = pd.read_csv("../in/Dummy 0.csv")
		for file_number in range(1,10):
			self.logger.debug("Loading CSV dummy file %s", file_number)
			df.append(pd.read_csv(f"../in/Dummy {file_number}.csv"))
		end = time.time()

		self.logger.info("End of loading the generated CSV files : %s", end - start)

	def loadCSVFilesAndConcatenate(self):
		"""On charge les fichiers CSV générés dans des DataFrame indépendants et on les concaténe en fin de traitement"""

		self.logger.info("Start of loading the generated CSV files (with DF concatenate)")

		start = time.time()
		df = []
		for file_number in range(10):
			self.logger.debug("Loading CSV dummy file %s", file_number)
			temp = pd.read_csv(f"../in/Dummy {file_number}.csv")
			df.append(temp)
		df = pd.concat(df, ignore_index=True)
		end = time.time()

		self.logger.info("End of loading the generated CSV files (with DF concatenate) : %s", end - start)

	def loadCSVFilesWithParallel(self):
		"""On charge les fichiers CSV générés, non un par un, mais en parallèle"""

		self.logger.info("Start of loading the generated CSV files (with Parallel)")

		start = time.time()
		def loop(file_number):
			return pd.read_csv(f"../in/Dummy {file_number}.csv")
		df = Parallel(n_jobs=-1, verbose=10)(delayed(loop)(file_number) for file_number in range(10))
		df = pd.concat(df, ignore_index=True)
		end = time.time()

		self.logger.info("End of loading the generated CSV files (with Parallel) : %s", end - start)

	def loadCSVFilesWithParallelAndThreads(self):
		"""On charge les fichiers CSV générés, non un par un, mais en parallèle"""

		self.logger.info("Start of loading the generated CSV files (with Parallel and Threads)")

		start = time.time()
		def loop(file_number):
			return pd.read_csv(f"../in/Dummy {file_number}.csv")
		df = Parallel(n_jobs=-1, verbose=10, prefer="threads")(delayed(loop)(file_number) for file_number in range(10))
		df = pd.concat(df, ignore_index=True)
		end = time.time()

		self.logger.info("End of loading the generated CSV files (with Parallel and Threads) : %s", end - start)

	def loadPickleFiles(self):
		"""On charge les fichiers Pickle générés"""

		self.logger.info("Start of loading the generated Pickle files (with Parallel)")

		start = time.time()
		def loop(file_number):
			return pd.read_pickle(f"../in/Dummy {file_number}.pickle")
		df = Parallel(n_jobs=-1, verbose=10)(delayed(loop)(file_number) for file_number in range(10))
		df = pd.concat(df, ignore_index=True)
		end = time.time()

		self.logger.info("End of loading the generated Pickle files (with Parallel) : %s", end - start)		

	def loadPickleFilesWithParallelAndThreads(self):
		"""On charge les fichiers Pickle générés"""

		self.logger.info("Start of loading the generated Pickle files (with Parallel and Threads)")

		start = time.time()
		def loop(file_number):
			return pd.read_pickle(f"../in/Dummy {file_number}.pickle")
		df = Parallel(n_jobs=-1, verbose=10, prefer="threads")(delayed(loop)(file_number) for file_number in range(10))
		df = pd.concat(df, ignore_index=True)
		end = time.time()

		self.logger.info("End of loading the generated Pickle files (with Parallel and Threads) : %s", end - start)		

	def loadExcelFilesInParallel(self):
		"""On charge les fichiers Excel générés en parallèle"""

		self.logger.info("Start of loading the generated Excel files (with Parallel)")

		start = time.time()
		def loop(file_number):
			return pd.read_excel(f"../in/Dummy {file_number}.xlsx")
		df = Parallel(n_jobs=-1, verbose=10)(delayed(loop)(file_number) for file_number in range(10))
		df = pd.concat(df, ignore_index=True)
		end = time.time()

		# Joblib autorise le changement de méthode de parallélisation. Il suffit d'ajouter prefer=”threads" à Parallel, et on devrait obtenir un gain de x4

		self.logger.info("End of loading the generated Excel files (with Parallel) : %s", end - start)		

	def loadExcelFilesInParallelWithThreads(self):
		"""On charge les fichiers Excel générés en parallèle"""

		self.logger.info("Start of loading the generated Excel files (with Parallel and Threads)")

		start = time.time()
		def loop(file_number):
			return pd.read_excel(f"../in/Dummy {file_number}.xlsx")
		df = Parallel(n_jobs=-1, verbose=10, prefer="threads")(delayed(loop)(file_number) for file_number in range(10))
		df = pd.concat(df, ignore_index=True)
		end = time.time()

		# Joblib autorise le changement de méthode de parallélisation. Il suffit d'ajouter prefer=”threads" à Parallel, et on devrait obtenir un gain de x4

		self.logger.info("End of loading the generated Excel files (with Parallel and Threads) : %s", end - start)		

	def loadFiles(self):
		"""On charge les différents fichiers générés selon différentes méthodes pour observer les résultats/performances."""

		self.logger.info("Start of loading the generated files")

		v.loadExcelFiles()
		v.loadCSVFiles()
		v.loadCSVFilesAndConcatenate()
		v.loadCSVFilesWithParallel()
		v.loadCSVFilesWithParallelAndThreads()
		v.loadPickleFiles()
		v.loadPickleFilesWithParallelAndThreads()
		v.loadExcelFilesInParallel()
		v.loadExcelFilesInParallelWithThreads()

		self.logger.info("End of loading the generated files")

if __name__ == "__main__":
	v = ExcelFileReader()
	v.init()
	v.loadFiles()