from dataclasses import dataclass
from query_template.headers import *
from tools import *

body_template = """
        [REPLACE_NAMESPACE_SELECTION]
        SELECT * {
            {
                SELECT
                    [REPLACE_GENERIC_SELECTION]
                WHERE
                {
                    [REPLACE_ENTITY_SELECTION]
                }
                GROUP BY [REPLACE_AGGREGATION_PROPERTIES]
                [REPLACE_HAVING_STATEMENT]
            }
        }
        [REPLACE LIMIT]
        [REPLACE SORTING]
        """

ontology_attribute_template = 'FILTER regex(STR([VARIABLE_NAME]), "[VARIABLE_VALUE]", "i")'
distinct_count_template = '(COUNT(DISTINCT [VARIABLE_NAME]) AS ?COUNT)'
sorting_template = 'ORDER BY DESC(?COUNT)'
having_template = 'HAVING (COUNT(DISTINCT ?base_entity) >= [REPLACE_MINSUPP])'

@dataclass
class variable:
    id : str

    name : str
    type : str

    unique_property_selector : str
    unique_property_value : str

    is_counted_variable : bool
    counted_variable_id : int

    optional_selector : str

    def __init__(self, id, name, type, unique_property_selector, unique_property_value, counted_variable=False, counted_variable_id = 0, optional_selector =''):
        self.id = id

        self.name = name
        self.type = type

        self.unique_property_selector = unique_property_selector
        self.unique_property_value = unique_property_value

        self.is_counted_variable = counted_variable
        self.counted_variable_id = counted_variable_id

        self.optional_selector = optional_selector

    def compile_name(self) -> str:
        if self.id is not None:
            return self.id

        if not self.is_counted_variable:
            return "?" + self.name
        else:
            return "?" + self.name + str(self.counted_variable_id)

    def compile_optional_selector(self):
        if self.optional_selector != '':
            return self.optional_selector.replace('[VARIABLE_NAME]', self.compile_name())
        return ''

    def compile_where(self) -> str:
        if self.id is not None:
            return ""


        if self.type is None:
            return stringify_statement(self.unique_property_selector.replace('[VARIABLE_NAME]', self.compile_name()).replace('[VARIABLE_VALUE]', self.unique_property_value)) + '\n' + self.compile_optional_selector()

        else:
            return stringify_statement(self.compile_name(), "rdf:type", self.type) + '\n' + self.compile_optional_selector()

@dataclass
class variable_connector:
    v1 : variable
    v2 : variable
    connector : str

    def __init__(self, v1, v2, conn:str):
        self.v1 = v1
        self.v2 = v2
        self.connector = conn

    def compile_where(self) ->str:
        return stringify_statement(self.v1.compile_name(), self.connector, self.v2.compile_name())


@dataclass
class sparql_apriori_template():
    header : str
    body : str

    variables : []
    connectors: []

    counting_variable : variable
    introduced_variable : variable
    sort : bool
    limit : int
    min_supp : int
    enable_endpoint_filter : bool

    def __init__(self, header, min_supp = 0,  limit=-1):
        self.header = header
        self.variables = []
        self.connectors = []

        self.counting_variable = None
        self.introduced_variable = None
        self.sort = True
        self.limit = limit
        self.enable_endpoint_filter = True
        self.min_supp = min_supp
        self.is_compiled = False
        self.body = body_template

    def compile_variable_where(self) -> str:
        return '\n'.join([v.compile_where() for v in self.variables])

    def compile_connector_where(self):
        return '\n'.join([c.compile_where() for c in self.connectors])


    def compile(self) -> str:
        if self.is_compiled: return self.body

        self.body = self.body.replace('[REPLACE_NAMESPACE_SELECTION]', self.header)

        self.body = self.body.replace('[REPLACE_ENTITY_SELECTION]', self.compile_variable_where() + "\n" + self.compile_connector_where())


        self.body = self.body.replace('[REPLACE_GENERIC_SELECTION]', self.introduced_variable.compile_name() + ' ' + distinct_count_template.replace('[VARIABLE_NAME]', self.counting_variable.compile_name()))
        self.body = self.body.replace('[REPLACE_AGGREGATION_PROPERTIES]', self.introduced_variable.compile_name())

        if self.limit != -1:
            self.body = self.body.replace('[REPLACE LIMIT]', 'LIMIT ' + str(self.limit))
        else:
            self.body = self.body.replace('[REPLACE LIMIT]', '')

        if self.sort:
            self.body = self.body.replace('[REPLACE SORTING]', sorting_template)
        else:
            self.body = self.body.replace('[REPLACE SORTING]', '')


        if self.enable_endpoint_filter:
            self.body = self.body.replace('[REPLACE_HAVING_STATEMENT]', having_template.replace('[REPLACE_MINSUPP]', str(int(self.min_supp))))
        else:
            self.body = self.body.replace('[REPLACE_HAVING_STATEMENT]', '')

        self.is_compiled = True
        return self.body
