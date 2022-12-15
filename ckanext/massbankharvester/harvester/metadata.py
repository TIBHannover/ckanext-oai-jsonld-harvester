from oaipmh.metadata import MetadataReader

json_container_reader = MetadataReader(
fields = {
    'json_data': ('textList','jc:json/text()'),

},
namespaces={
        'oai' : 'http://www.openarchives.org/OAI/2.0/',
        #'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
        'jc':'http://denbi.de/schemas/json-container',

   }
)