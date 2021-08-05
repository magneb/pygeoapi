# =================================================================
#
# Authors: Magne Bugten <magne.bugten@gmail.com>
#
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
# import json
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.sql import SQL, Identifier, Literal
from pygeoapi.provider.base import BaseProvider, \
    ProviderConnectionError, ProviderQueryError, ProviderItemNotFoundError

# Re-use the DatabaseConnection class from the postgresql privider package!
from pygeoapi.provider.postgresql import DatabaseConnection
#from pygeoapi.provider.postgresql import get_fields

# from psycopg2.extras import RealDictCursor
#from pygeoapi.provider.postgresql import DatabaseConnection, PostgreSQLProvider
# from pygeoapi.provider.xarray_ import _to_datetime_string, XarrayProvider

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
        self.instances = []
        
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
            self.fields = [str(num) for num in range(1, self.num_bands+1)]
            self.native_format = provider_def['format']['name']
            
        except Exception as err:
            LOGGER.warning(err)
            raise ProviderConnectionError(err)
        
    def get_coverage_domainset(self, *args, **kwargs):
        """Provide coverage domainset in json format
        The domainSet describes the direct positions of the coverage,
        i.e., the locations for which values are available.

        Returns:
        :returns: CIS JSON object of domainset metadata
        """
        LOGGER.debug('get_coverage_domainset triggered')
        domainset = {
            'type': 'DomainSetType',
            'generalGrid': {
                'type': 'GeneralGridCoverageType',
                'srsName': 4326,
                'axisLabels': [
                    'lat',
                    'lon'
                ],
                'axis': [{
                    'type': 'RegularAxisType',
                    'axisLabel': self._coverage_properties['x_axis_label'],
                    'lowerBound': self._coverage_properties['bbox'][0],
                    'upperBound': self._coverage_properties['bbox'][2],
                    'uomLabel': self._coverage_properties['bbox_units'],
                    'resolution': self._coverage_properties['resx']
                }, {
                    'type': 'RegularAxisType',
                    'axisLabel': self._coverage_properties['y_axis_label'],
                    'lowerBound': self._coverage_properties['bbox'][1],
                    'upperBound': self._coverage_properties['bbox'][3],
                    'uomLabel': self._coverage_properties['bbox_units'],
                    'resolution': self._coverage_properties['resy']
                }],
                'gridLimits': {
                    'type': 'GridLimitsType',
                    'srsName': 'http://www.opengis.net/def/crs/OGC/0/Index2D',
                    'axisLabels': ['i', 'j'],
                    'axis': [{
                        'type': 'IndexAxisType',
                        'axisLabel': 'i',
                        'lowerBound': 0,
                        'upperBound': self._coverage_properties['width']
                    }, {
                        'type': 'IndexAxisType',
                        'axisLabel': 'j',
                        'lowerBound': 0,
                        'upperBound': self._coverage_properties['height']
                    }]
                }
            },
            '_meta': {
                'tags': self._coverage_properties['tags']
            }
        }
        LOGGER.debug('Domainset:')
        LOGGER.debug(domainset)
        return domainset
    
    def get_coverage_rangetype(self, *args, **kwargs):
        """Provide coverage rangetype

        The rangeType element describes the structure and semantics of a coverage's range values, including (optionally) restrictions on the interpolation allowed on such values.
        
        :returns: CIS JSON object of rangetype metadata
        """
        LOGGER.debug('get coverage rangetype:')
        if True:
            with DatabaseConnection(self.conn_dic, self.table) as db:
                # self.fields = db.fields
                cursor = db.conn.cursor(cursor_factory=RealDictCursor)
                cursor.execute(f" \
                    select \
                        c.ordinal_position, c.column_name, \
                        c.data_type, pgd.description \
                    from information_schema.columns c \
                    join pg_catalog.pg_statio_all_tables as st \
                        on c.table_name=st.relname \
                    full outer join pg_catalog.pg_description pgd \
                        on(pgd.objsubid=c.ordinal_position and pgd.objoid=st.relid) \
                    where c.table_name='{self.table}';\
                    ")
                results = cursor.fetchall()
        
        rangetype = {
            'type': 'DataRecordType',
            'field': []
        }
            
        for result in results:
            LOGGER.debug('Determining rangetype for {}'.format(
                result['column_name']))
            
            ordinal, name, type, comment = result.values()

            # this is perhaps a little stripped down...
            rangetype['field'].append({
                'id': ordinal,
                'type': 'QuantityType',
                'name': name,
                'definition': comment,
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
            })
            
        return rangetype
    
    def get_fields(self):
        """
        Get fields from PostgreSQL table (columns are field)

        :returns: dict of fields
        """
        """
        if not self.fields:
            with DatabaseConnection(self.conn_dic, self.table) as db:
                self.fields = db.fields
        """
        return self.get_coverage_rangetype()
    
    def get_instance(self, instance):
        """
        Validate instance identifier

        :returns: `bool` of whether instance is valid
        """

        return NotImplementedError()

    def get_query_types(self):
        """Provide supported query types

        :returns: list of EDR query types
        """

        return ['position', 'area']
    
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

    def query(self, startindex=0, limit=1, bbox=[], properties=[], **kwargs):
        """Query Postgis for all the content.
        e,g: http://localhost:5000/collections/hotosm_bdi_waterways/items?
        limit=1&resulttype=results

        Args:
            startindex (int, optional): [description]. Defaults to 0.
            limit (int, optional): [description]. Defaults to 10.
        """
        query_params = {}
        
        LOGGER.debug('Querying PostGIS')
        
        LOGGER.debug('Query parameters: {}'.format(kwargs))

        LOGGER.debug('Query type: {}'.format(kwargs.get('query_type')))
        
        datetime_ = kwargs.get('datetime_')
        if datetime_ is not None:
            query_params[self._coverage_properties['time_axis_label']] = datetime_  # noqa
            

        with DatabaseConnection(self.conn_dic, self.table) as db:
            cursor = db.conn.cursor(cursor_factory=RealDictCursor)
            
            where_clause = self.__get_where_clauses(
                properties=properties, bbox=bbox
            )
        pass 
    
    # Helper functions will be at the end 
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
                        "select \
                        max(ST_numbands(rast)) as rast_bands \
                        from prediction_grids \
                        ")
                    cursor.execute(sql_query)
                    results = cursor.fetchall()
                    self.num_bands = results[0]['rast_bands']
                    LOGGER.debug(f'Number of raster bands: {self.num_bands}')

                # extract the bounds asuming all rasters have the same bounds..?
                if True:  # why?
                    sql_query = SQL(
                        "select \
                        st_extent(st_envelope(rast)) as bbox \
                        from prediction_grids pg \
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
                        "select \
                        st_width(rast) as rast_width, \
                        st_height(rast) as rast_height \
                        from prediction_grids pg \
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
            'time_axis_label': 'hours',
            'width': width,
            'height': height,
            'resx': resx,
            'resy': resy,
            'num_bands': self.num_bands,
            'tags': []
        }

        return properties
