# =================================================================
#
# Authors: Magne Bugten <magne.bugten@gmail.com>
#          Jorge Samuel Mendes de Jesus <jorge.dejesus@protonmail.com>
#          Tom Kralidis <tomkralidis@gmail.com>
#          Mary Bucknell <mbucknell@usgs.gov>
#
# Copyright (c) 2018 Jorge Samuel Mendes de Jesus
# Copyright (c) 2021 Tom Kralidis
# Copyright (c) 2021 Magne Bugten
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

from contextlib import nullcontext
import logging
from types import LambdaType
from numpy import datetime_as_string
# import json
# import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.sql import SQL, Identifier, Literal
from pygeoapi.provider.base import BaseProvider, \
    ProviderConnectionError, ProviderQueryError, \
    ProviderItemNotFoundError

# Re-use the DatabaseConnection class from the postgresql privider package!
from pygeoapi.provider.postgresql import DatabaseConnection, PostgreSQLProvider 

# from pygeoapi.provider.xarray_ import XarrayProvider
from pygeoapi.provider.rasterio_ import RasterioProvider
from rasterio.io import MemoryFile
import rasterio

from dataclasses import dataclass

@dataclass
class CoverageObject:
    """Store or handle data that represents a coverage."""
    domainType: str # -> ['Grid', 'Point', 'PointSeries']
    crs: int
    
    def add_feature(self, feature): 
        pass
    
    def set_geom(self, geom):
        if 'Grid' in self.domainType:
            # make grid 
            pass
        elif 'Point' in self.domainType:
            # make point geom
            pass
        else:
            return ValueError(self.domainType)
            


LOGGER = logging.getLogger(__name__)

class PostGISRasterProvider(BaseProvider):
    """EDR Provider for postgis raster"""

    def __init__(self, provider_def):
        """
        PostGISRasterProvider Class constructor
        
        :param provider_def: provider definitions from yml

        :returns: pygeoapi.provider.postgis_edr.PostGISRasterProvider
        """
        
        
        super().__init__(provider_def)

        # Get fields from the config yml
        self.table = provider_def['table']
        self.id_field = provider_def['id_field']
        self.conn_dic = provider_def['data']
        self.rast = provider_def['rast_field']
        self.instances = [] # ?
        self.field_uom = provider_def['field_uom']
        self.raster_parameters = provider_def['raster_parameters']
        
        
        # self.coverage = CoverageObject()
        
        
        LOGGER.debug('Setting Postgresql properties:')
        LOGGER.debug('Connection String:{}'.format(
            ",".join(("{}={}".format(*i) for i in self.conn_dic.items()))))
        LOGGER.debug('Name:{}'.format(self.name))
        LOGGER.debug('ID_field:{}'.format(self.id_field))
        LOGGER.debug('Table:{}'.format(self.table))
        
        try:
            # Get the fields from the database table
            self.fields = self.get_fields()
            # LOGGER.debug(f'Available fields: \n{self.fields}')
            
            # Get some coverage properties
            self._coverage_properties = self._get_coverage_properties()
            LOGGER.debug(f'properties: \n{self._coverage_properties}')
            
            self.axes = [self._coverage_properties['x_axis_label'],
                         self._coverage_properties['y_axis_label'],
                         self._coverage_properties['time_axis_label']]
            self.crs = self._coverage_properties['bbox_crs']
            self.num_bands = self._coverage_properties['num_bands']
            #self.fields = [str(num) for num in range(1, self.num_bands+1)]
            self.native_format = provider_def['format']['name']
            
        except Exception as err:
            LOGGER.warning(err)
            raise ProviderConnectionError(err)
    
    def get_fields(self):
        """create fields json response
        Get fields from PostgreSQL table (columns are field?)

        :returns: dict of fields
        """
        LOGGER.debug('get_fields() triggered!')
        
        if not self.fields:
            with DatabaseConnection(self.conn_dic, self.table) as db:
                cursor = db.conn.cursor(cursor_factory=RealDictCursor)
                cursor.execute(f" \
                    select \
                        c.ordinal_position, \
                        c.column_name, \
                        c.data_type, \
                        pgd.description \
                    from information_schema.columns c \
                    join pg_catalog.pg_statio_all_tables as st \
                        on c.table_name=st.relname \
                    full outer join pg_catalog.pg_description pgd \
                        on (pgd.objsubid=c.ordinal_position and \
                            pgd.objoid=st.relid) \
                    where c.table_name='{self.table}'\
                        and udt_name != 'geometry'\
                        and udt_name != 'raster';\
                    ")
                results = cursor.fetchall()
            
            self.fields = {
                'type': 'DataRecordType',
                'field': []
            }
                
            for result in results:
                LOGGER.debug('Setting field {}'.format(
                    result['column_name']))
                
                ordinal, name, type, comment = result.values()

                # this is perhaps a little stripped down...
                self.fields['field'].append({
                    'id': ordinal,
                    'type': type,
                    'name': name,
                    'definition': comment
                })
                """, # this came from the rangetype def. not sure if useful?
                    'nodata': 'null',
                    'uom': {
                        'id': 'http://www.opengis.net/def/uom/UCUM/{}'.format(
                            type),
                        'type': 'UnitReference',
                        'code': type
                    },
                    '_meta': {
                        'tags': []
                    }
                })"""
                
            # define aditional fields: 
            for field in self.raster_parameters:
                ordinal += 1
                self.fields['field'].append(self.raster_parameters[field])
                
        return self.fields
    
    def get_instance(self, instance):
        """
        Validate instance identifier
        in this case, instaces are really band numbers. 

        :returns: `bool` of whether instance is valid
        
        if instance in self.instances:
            return True
        else:
            return False
        """
        return NotImplementedError() 

    def get_query_types(self):
        """Provide supported query types

        :returns: list of EDR query types
        """

        return ['position', 'area']
    
    def query(self, **kwargs):
        """Query Postgis for all the content.
        e,g: http://localhost:5000/collections/hotosm_bdi_waterways/items?
        limit=1&resulttype=results

        Parameters are set by the ogc api edr standard and defined in 
        pygeoapi.api.API.get_collection_edr_query() by the variable "query_args"
                
        :param query_type: query type (one of items in get_query_type)
        :param wkt: `shapely.geometry` WKT geometry
        :param datetime_: temporal (datestamp or extent)
        :param select_properties: list of parameters to select from db
        :param z: vertical level(s)
        :param instance: data instance ?
        :param format_: data format of output
        
        :returns: coverage data as dict of CoverageJSON or native format
        """
        LOGGER.debug('Postgis-edr query:')
        # Validate required input
        try:
            all([kwargs.get('query_type'), 
                 kwargs.get('datetime_'),
                 kwargs.get('coords')])
        except:
            raise ValueError('parameter requirements not met!')
            
        out_meta = {} # accumulate metadata for output
        query_params = {}

        LOGGER.debug(f'Query parameters: \n{kwargs}')
        LOGGER.debug('Query type: {}'.format(kwargs.get('query_type')))

        LOGGER.debug(f"datetime arg: {kwargs.get('datetime_')}")
        where_datetime = self.handle_datetime(kwargs.get('datetime_'), 
                                              time_col=self.time_field)
            
        # process wkt input parameter: 

        query_params = self._process_wkt_parameter(kwargs.get('wkt'))
        LOGGER.debug(f'WKT query parameter: {query_params}')
        LOGGER.debug(f"WKT query parameter: {kwargs.get('wkt')}")
        
        # Run an if to construct the query before starting the database 
        # connection so that we dont need so much duplicate code...
        
        # Handle position queries
        if kwargs.get('query_type') == 'position': 
            # needs a where clause and a query 
            where_clause = f"where \"useCaseId\" = 'MFMC01' \
                and {where_datetime} \
                order by \"{self.time_field}\" DESC"
            
            sql_query = SQL(f"\
                SELECT \
                    pid, \
                    \"timeTag\", \
                    \"timeHorizon\", \
                    \"resultType\", \
                    \"solarWindDataSource\", \
                    \"ionosphereHeight\", \
                    \"useCaseName\", \
                    \"useCaseId\", \
                    \"threshold_ROTI\", \
                    \"threshold_PosErr\", \
                    \"Bx\", \
                    \"By\", \
                    \"Bz\", \
                    n, \
                    v, \
                    ST_Value(\
                        {self.rast}, 1, ST_SetSRID(\
                            ST_GeomFromText('{kwargs.get('wkt')}'), 4326)) \
                                as \"pROTI\"\
                    from {self.table} \
                    {where_clause}")
            
            # Run query
            row_data = self._fetch_from_database(sql_query)
            
        # Handle area queries
        elif kwargs.get('query_type') == 'area':
            LOGGER.error('unsupported query type!')
            where_clause = f"where \"useCaseId\" = 'MFMC01' \
                and {where_datetime} \
                ORDER BY \"{self.time_field}\" DESC LIMIT 1"
                
            sql_query = SQL(f"\
                with\
                    input_polygon as (\
                        select\
                            ST_SetSRID(ST_GeomFromText('{kwargs.get('wkt')}'),\
                            4326) as geom)\
                SELECT \
                    pid, \
                    \"useCaseName\", \
                    n, \
                    v, \
                    \"{self.time_field}\" as datetime_, \
                    ST_AsGDALRaster(\
                        ST_Clip({self.rast}, 1, geom, true), 'GTiff') as rast \
                    from {self.table}, input_polygon \
                    {where_clause}")
            
            # Run query
            row_data = self._fetch_from_database(sql_query)
            
            
        else:
            LOGGER.error('unsupported query type!')
            return ('unsupported query type!')
        

        # for the domain (x,y), provide the array and the timestamps 
        # for the data, such that, covjson can structure the geometry first? 
        # does this have any disadvantages? 
        if kwargs.get('query_type') == 'position':
            # For the position query type, lets return both 
            # a Point and a PointSeries domain
            if len(row_data) == 1:
                pass
            else:
                pass
        elif kwargs.get('query_type') == 'area':
            # TODO: restructure and cleanup. 
            # assume that area queries only return a single instance coverage!
            data_dict = {}
            coverage = row_data[0]
            LOGGER.debug(f"Coverage: \n{coverage}")
            # copy from memoryview? 
            rast = coverage['rast'].tobytes()
            LOGGER.debug(f"raster data type: {type(rast)}")
            with MemoryFile(rast).open(driver='GTiff') as dataset:
                self._data = dataset
                data_dict[coverage['datetime_']] = dataset.read()
                out_meta = dataset.meta
                bbox = dataset.bounds
                out_meta['bbox'] = [bbox[0], bbox[1], bbox[2], bbox[3]]
                #item['rast'] = data_array
                out_meta['bands'] = [0]
                LOGGER.debug(f'Meta: {out_meta}')
                LOGGER.debug(f'bbox: {bbox}')

                    # this return really should include all items that were 
                    # returned by the database. but i cant make that work yet     
                """     return RasterioProvider.gen_covjson(self, 
                                                    out_meta, 
                                                    list(data_dict.values())[0]) """
                return self.gen_area_covjson(out_meta, list(data_dict.values())[0])
        else:
            return None
        return None

    
    def handle_datetime(self, requested_datetimes, time_col="timeTag"):
        """convert an edr time request to sql
        EDR time requests are strings in the format [datetime][/[datetime]]
        where either is optional and an open ended datatime is denoted by '..'
        
        Datetime is allowed in a couple of forms:
        - datetime : spesiffic time
        - datetime/.. OR ../datetime : open ended from or to
        - datetime/datetime : closed from or to.
        
        args:
            requested_datetimes: string of request
            time_col: database column of time tag. 
        returns:
            string: used directly in where clause
        """
        LOGGER.debug(f'Requested datetime: {requested_datetimes}')
        if not '/' in requested_datetimes:  # instance
            return f"\"{time_col}\" = '{requested_datetimes}'"
        else:  # datetime envelope
            start, end = requested_datetimes.split('/')
            if ".." in start:
                return f"\"{time_col}\" >= '{end}'"
            elif ".." in end:
                return f"\"{time_col}\" <= '{start}'"
            else:
                return f"\"{time_col}\" <= '{start}'' and \
                         \"{time_col}\" >= '{end}'"
                         
    def gen_area_covjson(self, metadata, data):
        """Generate coverage as CoverageJSON representation

        This is for a single parameter area query!

        :param metadata: coverage metadata
        :param data: rasterio DatasetReader object
        :param range_type: range type list

        :returns: dict of CoverageJSON representation
        """

        LOGGER.debug('Creating CoverageJSON domain')
        minx, miny, maxx, maxy = metadata['bbox']

        domainType = 'Grid'
        axes = {
            'x': {
                'start': minx,
                'stop': maxx,
                'num': metadata['width']
            },
            'y': {
                'start': maxy,
                'stop': miny,
                'num': metadata['height']
            },
            'z' : { # ['pROTI', 'pPosErr']
                'values' : ['pROTI']#raster_bands
                },
            't': {
                'values': ["2021-08-19T08:25:00Z"]# response_timestamp
            }
        }
        referencing = [{
            'coordinates': ['x', 'y'],
            'system': {
                'type': self._coverage_properties['crs_type'],
                'id': self._coverage_properties['bbox_crs']
            }
        }, {
            "coordinates": ["t"],
            "system": {
                "type": "TemporalRS",
                        "calendar": "Gregorian"
            }
        }]

        cj = {
            'type': 'Coverage',
            'domain': {
                'type': 'Domain',
                'domainType': domainType,
                'axes': axes,
                'referencing': referencing
            },
            'parameters': {},
            'ranges': {}
        }
        
        # expect this to be [0]
        if metadata['bands'] is None:  # all bands
            bands_select = range(1, len(self._data.dtypes) + 1)
        else:
            bands_select = metadata['bands']

        LOGGER.debug('bands selected: {}'.format(bands_select))
        for bs in bands_select:
            pm = self._get_parameter_metadata(
                self._data.profile['driver'], self._data.tags(bs))

            parameter = {
                'type': 'Parameter',
                'description': pm['description'],
                'unit': {
                    'symbol': pm['unit_label']
                },
                'observedProperty': {
                    'id': pm['observed_property_id'],
                    'label': {
                        'en': pm['observed_property_name']
                    }
                }
            }

            cj['parameters'][pm['id']] = parameter
        
        try:
            for key in cj['parameters'].keys():
                cj['ranges'][key] = {
                    'type': 'NdArray',
                    # 'dataType': metadata.dtypes[0],
                    'dataType': 'float',
                    'axisNames': ['y', 'x'],
                    'shape': [metadata['height'], metadata['width']],
                }
                # TODO: deal with multi-band value output
                cj['ranges'][key]['values'] = data.flatten().tolist()
        except IndexError as err:
            LOGGER.warning(err)
            raise ProviderQueryError('Invalid query parameter')

        """
        cj['ranges'][metadata.parameter['id']] = {
            'type': 'NdArray',
            'dataType': str(metadata['dtype']),
            'axisNames': [
                'y', 'x', 't'
            ],
            'shape': [metadata['height'],
                      metadata['width'],
                      1]
        }

        data = data.fillna(None)
        cj['ranges'][metadata.parameter['id']]['values'] \
            = data[metadata.parameter['id']].values.flatten().tolist()  # noqa
        """
        return cj

    @staticmethod
    def _get_parameter_metadata(driver, band):
        """
        Helper function to derive parameter name and units
        :param driver: rasterio/GDAL driver name
        :param band: int of band number
        :returns: dict of parameter metadata
        """

        parameter = {
            'id': None,
            'description': None,
            'unit_label': None,
            'unit_symbol': None,
            'observed_property_id': None,
            'observed_property_name': None
        }

        if driver == 'GRIB':
            parameter['id'] = band['GRIB_ELEMENT']
            parameter['description'] = band['GRIB_COMMENT']
            parameter['unit_label'] = band['GRIB_UNIT']
            parameter['unit_symbol'] = band['GRIB_UNIT']
            parameter['observed_property_id'] = band['GRIB_SHORT_NAME']
            parameter['observed_property_name'] = band['GRIB_COMMENT']
            
        if driver == 'GTiff':
            parameter['id'] = 'pROTI'
            parameter['description'] = 'hardcoded description'
            parameter['unit_label'] = 'unit'
            parameter['unit_symbol'] = 'symbol'
            parameter['observed_property_id'] = 'pROTI'
            parameter['observed_property_name'] = 'atmospheric ionization'

        return parameter
    
    def gen_point_covjson(self, metadata, data):
        """Generate coverage as CoverageJSON representation

        This is for a multi parameter point query!

        :param metadata: coverage metadata
        :param data: rasterio DatasetReader object
        :param range_type: range type list

        :returns: dict of CoverageJSON representation
        """

        LOGGER.debug('Creating CoverageJSON domain')
        
        
        if len(data) == 1:
            domainType = 'Point'
        else:
            domainType = 'PointSeries'
        axes = {
                    'x': {"values": [x]},
                    'y': {"values": [y]},
                    't': {"values": data['_datetime'].values()}
                }   
        referencing = [{
                    'coordinates': ['x', 'y'],
                    'system': {
                        'type': self._coverage_properties['crs_type'],
                        'id': self._coverage_properties['bbox_crs']
                    }
                }, {
                    "coordinates": ["t"],
                    "system": {
                        "type": "TemporalRS",
                        "calendar": "Gregorian"
                    }
                }]

        cj = {
            'type': 'Coverage',
            'domain': {
                'type': 'Domain',
                'domainType': domainType,
                'axes': axes,
                'referencing': referencing
            },
            'parameters': {},
            'ranges': {}
        }
        
        cj['parameters'][metadata.parameter['id']] = metadata.parameter

        cj['ranges'][metadata.parameter['id']] = {
            'type': 'NdArray',
            'dataType': str(self._data[variable].dtype),
            'axisNames': [
                'y', 'x', 't'
            ],
            'shape': [metadata['height'],
                        metadata['width'],
                        1]
        }

        data = data.fillna(None)
        cj['ranges'][metadata.parameter['id']]['values'] \
            = data[metadata.parameter['id']].values.flatten().tolist()  # noqa

        return cj
    # Helper functions will be at the end 
    
    def _fetch_from_database(self, sql_query):
        """Helper function to call database

        Args:
            sql_query (string): complete sql query string or SQL object?

        Raises:
            ProviderQueryError: problem with cursor execusion

        Returns:
            list: list of lists containing database responses
        """

        # Start database query
        with DatabaseConnection(self.conn_dic, self.table) as db:
            cursor = db.conn.cursor(cursor_factory=RealDictCursor)

            LOGGER.debug('SQL Query: {}'.format(sql_query.as_string(cursor)))

            # probably unnessecary try clause
            try:
                LOGGER.debug('Executing...')
                cursor.execute(sql_query)
            except Exception as err:
                LOGGER.error('Error executing sql_query: {}'.format(
                    sql_query.as_string(cursor)))
                LOGGER.error(err)
                raise ProviderQueryError()

            LOGGER.debug('Fetching result from database...')
            row_data = cursor.fetchall()
            #LOGGER.debug(row_data)
        return row_data


    
    def _process_wkt_parameter(self, wkt):
        query_params = {}
        if wkt is not None:
            LOGGER.debug('Processing WKT')
            LOGGER.debug('Geometry type: {}'.format(wkt.type))
            if wkt.type == 'Point':
                query_params[self._coverage_properties[
                    'x_axis_label']] = wkt.x
                query_params[self._coverage_properties[
                    'y_axis_label']] = wkt.y
            elif wkt.type == 'LineString':
                query_params[self._coverage_properties[
                    'x_axis_label']] = wkt.xy[0]  # noqa
                query_params[self._coverage_properties[
                    'y_axis_label']] = wkt.xy[1]  # noqa
            elif wkt.type == 'Polygon':
                query_params[self._coverage_properties[
                    'x_axis_label']] = slice(
                        wkt.bounds[0], wkt.bounds[2])  # noqa
                query_params[self._coverage_properties[
                    'y_axis_label']] = slice(
                        wkt.bounds[1], wkt.bounds[3])  # noqa
                pass
            
        return query_params
    
    def _get_datetime_records(self):
        """Fetch all avaliable datetimes from the database
        or atleast some of them? 
        
        returns: 'ordered' list of datetime objects
            
        """
        with DatabaseConnection(self.conn_dic, self.table) as db:
                cursor = db.conn.cursor(cursor_factory=RealDictCursor)
                cursor.execute(f'select \
                               distinct "{self.time_field}" \
                               from {self.table} \
                               order by "{self.time_field}" desc \
                               ')
                results = cursor.fetchall()
        
        return [result[f"{self.time_field}"] for result in results]
    
    def __get_where_clauses(self, properties=[], bbox=[]):
        """Generarates WHERE conditions to be implemented in query.
        Private method mainly associated with query method
        Not sure if strictly nessecary??
        
        :param properties: list of tuples (name, value)
        :param bbox: bounding box [minx,miny,maxx,maxy]

        :returns: psycopg2.sql.Composed or psycopg2.sql.SQL
        """

        where_conditions = []
        if properties:
            property_clauses = [SQL('{} = {}').format(
                Identifier(k), Literal(v)) for k, v in properties]
            where_conditions += property_clauses
        if bbox:
            bbox_clause = SQL('{} && ST_MakeEnvelope({})').format(
                Identifier(self.rast), SQL(', ').join(
                    [Literal(bbox_coord) for bbox_coord in bbox]))
            where_conditions.append(bbox_clause)

        if where_conditions:
            where_clause = SQL(' WHERE {}').format(
                SQL(' AND ').join(where_conditions))
        else:
            where_clause = SQL('')
            LOGGER.debug('')

        return where_clause
    
    def _get_coverage_properties(self):
        """Helper function to normalize coverage properties
        
        Adapted from rasterio_
        maybe add from xarray instead? 
        
        :returns: `dict` of coverage properties
        """
        LOGGER.debug('_get_coverage_properties triggered')

        if not self.properties:
            with DatabaseConnection(conn_dic=self.conn_dic, table=self.table, context='query') as db:
                LOGGER.debug('Database connection established!')
                cursor = db.conn.cursor(cursor_factory=RealDictCursor)

                # get the number of bands if not avaliable
                if not self.num_bands:
                    sql_query = SQL(
                        f"select \
                        max(ST_numbands({self.rast})) as rast_bands \
                        from {self.table} \
                        ")
                    cursor.execute(sql_query)
                    results = cursor.fetchall()
                    self.num_bands = results[0]['rast_bands']
                    LOGGER.debug(f'Number of raster bands: {self.num_bands}')

                # extract the bounds asuming all rasters have the same bounds..?
                if True:  # why?
                    sql_query = SQL(
                        f"select \
                        st_extent(st_envelope({self.rast})) as bbox \
                        from {self.table} \
                        limit 1 \
                        ")
                    cursor.execute(sql_query)
                    results = cursor.fetchall()
                    bbox = results[0]['bbox'][4:-1]
                    lower_left, upper_right = bbox.split(',')
                    left, bottom = map(float, lower_left.split(' '))
                    right, top = map(float, upper_right.split(' '))
                    LOGGER.debug(
                        f'Data bounds: {left}, {top}, {right}, {bottom}')

                # extract the widt and height of the raster
                if True:
                    sql_query = SQL(
                        f"select \
                        st_width({self.rast}) as rast_width, \
                        st_height({self.rast}) as rast_height \
                        from {self.table} \
                        limit 1 \
                        ")
                    cursor.execute(sql_query)
                    results = cursor.fetchall()

                    width = results[0]['rast_width']
                    height = results[0]['rast_height']

            # other:
            #resulution unit per pixel
            resx = (right - left)/width
            resy = (top-bottom)/height

        # In this implementation, some properties will be set manually.
        # this is double pluss un-good.
        properties = {
            'bbox': [
                left,
                bottom,
                right,
                top
            ],
            'bbox_crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84',
            'crs_type': 'GeographicCRS',
            'bbox_units': 'deg',
            'x_axis_label': 'Long',
            'y_axis_label': 'Lat',
            'time_axis_label': 'datetime', # get from column info?
            'width': width,
            'height': height,
            'resx': resx,
            'resy': resy,
            'num_bands': self.num_bands,
            'tags': []
        }

        return properties

if __name__ == "__main__":
    coverage = CoverageObject('Grid', 4326)
    print(coverage)
    coverage.set_geom('lol')
