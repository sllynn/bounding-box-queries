-- Databricks notebook source
-- MAGIC %md ### Query profiler?

-- COMMAND ----------

-- MAGIC %sh wget --output-document /tmp/taxi_zones.csv https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD

-- COMMAND ----------

response = requests.get(
  url=f"https://{host}/api/2.0/preview/scim/v2/Users",
        headers=dict(Authorization=f"Bearer {token}"),
        params=dict(
          filter="userName eq stuart.lynn@databricks.com"
        )
)

userid = response.json()["Resources"][0]["id"]
userid

-- COMMAND ----------

from pyspark.sql import SparkSession
from typing import Optional

class LoggerProvider:
    def get_logger(self, spark: SparkSession, custom_prefix: Optional[str] = ""):
        log4j_logger = spark._jvm.org.apache.log4j  # noqa
        return log4j_logger.LogManager.getLogger(custom_prefix + self.__full_name__())

    def __full_name__(self):
        klass = self.__class__
        module = klass.__module__
        if module == "__builtin__":
            return klass.__name__  # avoid outputs like '__builtin__.str'
        return module + "." + klass.__name__

-- COMMAND ----------

from dataclasses import dataclass, asdict

@dataclass
class SQLParameter:
    name: str
    value: str
    type: str

class QueryAnalyser:

    def __init__(self, spark: SparkSession, user_id: int, warehouse_id: str, catalog: str, schema: str):
        notebook_context = (
            dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        )
        self._host = notebook_context.browserHostName().get()
        self._token = notebook_context.apiToken().get()
        self._headers = dict(Authorization=f"Bearer {self._token}")

        self._user_id = user_id
        self._warehouse_id = warehouse_id
        self._catalog = catalog
        self._schema = schema

        self._statement = ""
        self._query_ids = []
        self._parameters = {}
        self._results = {}

        loggingProvider = LoggerProvider()
        self.logger = loggingProvider.get_logger(spark)

    @property
    def statement(self):
        return self._statement

    @statement.setter
    def statement(self, value):
        self._statement = value

    @property
    def query_parameters(self):
        return self._parameters.values()

    def add_query_parameter(self, parameter: SQLParameter):
        if not parameter.name in list(self._parameters.keys()):
            self._parameters.update({parameter.name:  parameter})
        else:
          raise Exception("Parameter already exists.")

    def update_parameter_value(self, name, value):
        if not self._parameters[name]:
            raise Exception("Parameter not found.")
        self._parameters[name].value = value

    @property
    def results(self):
      return self._results

    def query(self):
        body = dict(
            warehouse_id=self._warehouse_id,
            catalog=self._catalog,
            schema=self._schema,
            parameters=[asdict(p) for p in self.query_parameters],
            statement=self._statement,
        )
        response = requests.post(
            url=f"https://{self._host}/api/2.0/sql/statements",
            headers=self._headers,
            data=json.dumps(body),
        )
        if response.status_code == 200:
            self._query_ids += [response.json()["statement_id"]]
        else:
          self.logger.warn(f"Query failed for statement {self.statement} and parameters {self.query_parameters}")

    def get_execution_times(self):
        response = requests.get(
            url=f"https://{self._host}/api/2.0/sql/history/queries",
            headers=self._headers,
            params={
                "filter_by.user_ids": self._user_id,
                "filter_by.warehouse_ids": self._warehouse_id,
                "max_results": 1000,
                "include_metrics": "true"
            },
        )
        self._results = {
            r["query_id"]: (r["status"], r["duration"], r["metrics"])
            for r in response.json()["res"]
            if r["query_id"] in self._query_ids
        }

-- COMMAND ----------

analyser = QueryAnalyser(
  spark=spark,
    user_id=8628367616981178, 
    warehouse_id="690e87af2b2769c4", 
    catalog="stuart",
    schema="bp_geo_perf"
)
analyser.statement = "select * FROM filter_trips(:query_geom) LIMIT :row_limit"
analyser.add_query_parameter(SQLParameter("row_limit", "1000", "INT"))
analyser.add_query_parameter(SQLParameter("query_geom", "", "STRING"))
analyser.update_parameter_value("query_geom", "POINT(0 0)")
analyser.query_parameters

-- COMMAND ----------

import pandas as pd

taxi_zones_df = pd.read_csv("/tmp/taxi_zones.csv")

def update_and_query(geom: str):
  analyser.update_parameter_value("query_geom", geom)
  analyser.query()

zones = taxi_zones_df["the_geom"].values
for zone in zones:
  update_and_query(geom=zone)

-- COMMAND ----------

-- MAGIC %md ### Appendix

-- COMMAND ----------

-- MAGIC %md ### Appendix

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC with e as (select OBJECTID, envelope(the_geom) as bbox from taxi_zones)
-- MAGIC select
-- MAGIC   e.bbox,
-- MAGIC   great_circle_distance(e.bbox.xmin, e.bbox.ymin, e.bbox.xmax, e.bbox.ymax) as diagonal_distance,
-- MAGIC   *
-- MAGIC from taxi_zones t inner join e using (OBJECTID)

-- COMMAND ----------

test_row = (
  _sqldf
  .orderBy(col("diagonal_distance").desc())
  .first()
  )

-- COMMAND ----------

test_geom = test_row.the_geom
test_distance = test_row.diagonal_distance

-- COMMAND ----------

test_geom

-- COMMAND ----------

coords = (
  test_geom
  .replace("MULTIPOLYGON ", "")
  .replace("(","")
  .replace(")","")
  .split(", ")
  )
coords

-- COMMAND ----------

import numpy as np

coords2 = [c.split(" ") for c in coords]
x_array = np.asarray([float(x) for x, _ in coords2])
y_array = np.asarray([float(y) for _, y in coords2])

-- COMMAND ----------

x_array.min(), y_array.min(), x_array.max(), y_array.max()

-- COMMAND ----------

(
  np.pi * x_array.min() / 180, 
  np.pi * y_array.min() / 180,
  np.pi * x_array.max() / 180,
  np.pi * y_array.max() / 180
  )

-- COMMAND ----------

6378137 * np.arccos(np.sin(0.7099751056308047) * np.sin(0.710328901742355) + 
          np.cos(0.7099751056308047) * np.cos(0.710328901742355) * np.cos(-1.2900828268183806 - -1.290565380232175))

-- COMMAND ----------

test_distance

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select great_circle_distance(-73.8782477932424, 40.58169379478867, -73.76670827781236, 40.639953274487794)

-- COMMAND ----------

test_row.bbox

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select envelope("MULTIPOLYGON (((-73.91741294299997 40.68403405199995, -73.9172663269999 40.68330322399991, -73.91712138499985 40.682568471999964, -73.9169763279999 40.68183415899995, -73.91683011499998 40.68110098099987, -73.91668638399985 40.680371378999965, -73.916539987 40.67963749699988, -73.91639300100002 40.678905848999904, -73.9163011989999 40.678577111999914, -73.91720270699984 40.678626814999916, -73.91796830599993 40.67866902199989, -73.91905083899984 40.678728473999875, -73.92164666399992 40.67886992299992, -73.92183500099996 40.67894555899992, -73.92197287199998 40.67907846999987, -73.9219999079998 40.679191413999895, -73.92201534599985 40.67925589999992, -73.92203214100005 40.67932605899988, -73.92205453499997 40.67941960399992, -73.92208787199982 40.679558857999915, -73.92211149999984 40.67965755699987, -73.92213242899994 40.67974497799986, -73.92215222999988 40.679844634999895, -73.92217715699984 40.6799700849999, -73.92221626999996 40.68016693499992, -73.92223681799994 40.680270342999904, -73.92225617599988 40.68036776399987, -73.92227774199988 40.6804762929999, -73.92231261899987 40.68064847299997, -73.92233849599998 40.680776217999885, -73.92235458599998 40.68085564499992, -73.92236962399983 40.68092987999992, -73.92238256000003 40.68099373699995, -73.92240197099996 40.68108955299994, -73.92242652099985 40.68121074499994, -73.92249266199997 40.681616108999926, -73.92250799299985 40.68171006999997, -73.9225447959999 40.68193561299989, -73.92267012199994 40.68268151199991, -73.92281374299995 40.68341442999991, -73.92609461699992 40.68303870999989, -73.92900883999985 40.68270290499991, -73.92916130699992 40.68343632399992, -73.93208669799994 40.6831003499999, -73.93500821699992 40.68276376799985, -73.93486257200001 40.68203056899991, -73.93471441099992 40.68129738399991, -73.93457098299989 40.68056476199989, -73.93450838999993 40.68022169299993, -73.93439530999997 40.67983537999992, -73.93435066499993 40.67956340799992, -73.93487210899985 40.679593087999855, -73.93739763699983 40.679729899999934, -73.93843613299987 40.67978585199994, -73.94032794 40.67988997499989, -73.94047634999994 40.68063569499987, -73.94062005399995 40.68137012999991, -73.94076893299986 40.6821008389999, -73.940913343 40.68283361699987, -73.94105783799992 40.68356687199986, -73.94120399299996 40.68429983899993, -73.94134827200003 40.68503120299989, -73.94149491799988 40.685764528999904, -73.94163933199995 40.686497269999954, -73.94178527599988 40.68722837199987, -73.94193245099987 40.68796295899991, -73.9420768489998 40.688697201999894, -73.94222203500003 40.68942797899993, -73.94236932699991 40.6901599449999, -73.94251587899991 40.690892000999916, -73.94266181699986 40.69162434399988, -73.94280765199987 40.69235779399988, -73.942951361 40.693090783999885, -73.94310040999994 40.69382302899988, -73.9431142779999 40.6938947199999, -73.94312826799984 40.6939670379999, -73.94324249099988 40.694557484999955, -73.943388021 40.69528898999993, -73.94352527499989 40.6960308549999, -73.94354024099975 40.696108141999964, -73.94355634200002 40.696191282999926, -73.94362121399985 40.696526278999876, -73.9436380689999 40.69661331799992, -73.94368427099982 40.69685190199992, -73.9437245359998 40.69705981199991, -73.9437430669999 40.69715549899994, -73.94378455600005 40.697369728999924, -73.94380383199999 40.69746926499997, -73.94391750199989 40.698056201999854, -73.94394947299996 40.698221278999924, -73.94299724499987 40.69833009799987, -73.9429206409998 40.698338850999974, -73.94253735900001 40.69838264999991, -73.94247471499993 40.698389807999945, -73.94185058700002 40.69846112399996, -73.94176673799987 40.69847070399986, -73.94115035299995 40.69854113199994, -73.94081408799997 40.69858111999987, -73.94072013299994 40.69859229399992, -73.9405942079999 40.698607267999904, -73.94051277999984 40.69861695199995, -73.94011148799991 40.69866467199989, -73.94002089599995 40.69867544399993, -73.93960973999992 40.69872433499995, -73.93952025999982 40.69873497499989, -73.93906425899979 40.698789194999925, -73.93896470899988 40.69880103199995, -73.93856854799981 40.69884813599989, -73.93817718599986 40.69864022299987, -73.93777354599992 40.69840469299989, -73.93739064799986 40.69818579499985, -73.93675978999995 40.69783118899989, -73.93638762599981 40.69761530199985, -73.93600679799988 40.69740574899992, -73.93522264399988 40.69698654499995, -73.93479699699994 40.69673659199992, -73.93468892700004 40.696674953999946, -73.93460675199995 40.696628084999865, -73.93457999399985 40.69661282399995, -73.93448784799989 40.696560267999914, -73.93413397299992 40.696358430999936, -73.93382817199998 40.69618427699988, -73.93311862899988 40.69579115399997, -73.93236605699987 40.69535837699995, -73.93192890099984 40.69511465699991, -73.93177404599997 40.69502821499992, -73.93145784300005 40.6948554709999, -73.93114516499988 40.69467397499987, -73.93051146699992 40.694316522999905, -73.9297019239999 40.69386339699993, -73.92882897899982 40.69335047399987, -73.9288283419999 40.693350098999936, -73.92864268999999 40.6932410129999, -73.92850659699988 40.693165725999904, -73.92804330699992 40.69290417199992, -73.92781120699985 40.692774208999936, -73.92750685299994 40.69260381099993, -73.92707069499986 40.692346223999955, -73.92644992099981 40.69200176899993, -73.92631612499989 40.691928452999925, -73.92556064099996 40.69149083999987, -73.92536488899991 40.69136634399996, -73.92439183300002 40.690831748999926, -73.92429213299994 40.69077335699992, -73.9236437979999 40.69039883799992, -73.9231696669999 40.690123405999884, -73.922807024 40.68992392499995, -73.92207133799984 40.68948984099993, -73.92130587799981 40.68907164399991, -73.92121369999991 40.68901947699987, -73.92096103899999 40.688872051999915, -73.92055013100003 40.68864092199994, -73.9198096189999 40.688211240999856, -73.91906109199991 40.68778436899987, -73.91878323299994 40.687626087999924, -73.91829345799982 40.687356667999936, -73.91804607 40.68721324799989, -73.91799580199995 40.686965380999894, -73.917915484 40.68656593599996, -73.91789200399997 40.68644915899997, -73.91784937499982 40.68623316699994, -73.91783531199984 40.686163668999875, -73.91778426799988 40.6859114119999, -73.91776547599996 40.68581854099989, -73.91770109499986 40.68550035899995, -73.91765638699994 40.68527188299991, -73.91764776599992 40.68522782299993, -73.91763742499995 40.685174975999885, -73.91762630599987 40.685118148999884, -73.91755763299994 40.68476719199989, -73.91741294299997 40.68403405199995)))")

-- COMMAND ----------


# import geopandas as gpd
# taxi_zones_df["geom_the_geom"] = gpd.GeoSeries.from_wkt(taxi_zones_df["the_geom"])
# taxi_zones_gdf = gpd.GeoDataFrame(taxi_zones_df, geometry="geom_the_geom")
# taxi_zones_gdf["bbox"] = taxi_zones_gdf.envelope.to_wkt()
# taxi_zones_sdf = spark.createDataFrame(taxi_zones_gdf.drop("geom_the_geom", axis=1))
