import StringIO
import unicodecsv as csv

import pylons

import ckan.plugins as p
import ckan.lib.base as base

from ckan.controllers.api import ApiController
import ckan.logic as logic
from sqlalchemy import *
from ckan.model import Resource, meta


from ckan.common import request

class DumpController(base.BaseController):
    def dump_action(self, resource_id):
        """Routes dataproxy type resources to dataproxy_dump method, else performs 'datastore_dump' action"""
        
        # TODO: No access control checks for dataproxy resources!
        resource     = Resource.get( resource_id )

        if resource is not None and resource.url_type == 'dataproxy':
            pylons.response.headers['Content-Type'] = 'text/csv'
            pylons.response.headers['Content-disposition'] = \
                'attachment; filename="{name}.csv"'.format(name=resource_id)
            return self.dataproxy_dump(request_data, resource_id)

        # Default action otherwise
        return self.action('dump', ver=3)


    def dataproxy_dump(self, resource_id):
        # TODO Do these get used?
        data_dict = {
            'resource_id': resource_id,
            'limit':       request.GET.get('limit', 100000),
            'offset':      request.GET.get('offset', 0)
        }

        try:
            result = self.dataproxy_search(request_data, resource_id)
        except p.toolkit.ObjectNotFound:
            base.abort(404, p.toolkit._('DataProxy resource not found'))
        
        f  = StringIO.StringIO()
        wr = csv.writer(f, encoding='utf-8')

        header = [ x['id'] for x in result['fields'] ]
        wr.writerow(header)

        for record in result['records']:
            wr.writerow([record[column] for column in header])
        return f.getvalue()