from collections import OrderedDict
import sqlite3
import logging


class Database:
    def __new__(cls, name=None, **kwargs):
        """ Singleton class object creation override. Pass database name to switch the databases.
            Use constructor without name to get current DB"""
        if name is None:
            if not hasattr(cls, 'instance'):
                raise Exception("Database not set. Please, set the database name before defining the models")
        else:
            cls.instance = super(Database, cls).__new__(cls)
        return cls.instance

    def __init__(self, name=None, journal='MEMORY'):
        if name is None:
            return
        self.name = name
        self.models = OrderedDict()
        self.connection = sqlite3.connect(self.name)
        logging.debug("Connecting to %s" % self.name)

    def __del__(self):
        logging.debug("Closing connection to %s" % self.name)
        self.connection.close()

    def execute(self, sql):
        logging.debug(sql)
        self.connection.execute(sql)

    def commit(self):
        logging.debug("Commiting db %s" % self.name)
        self.connection.commit()

    def init(self, not_exists=False):
        for model_name, model_obj in self.models.items():
            sql_query_part = ", ".join((("%s" % field.__get_sqlite_def__())
                                            for (name, field) in model_obj.fields.items()))
            if_not_exists =" IF NOT EXISTS" if not_exists else ""
            sql_query = "CREATE TABLE%s %s (%s);" % (if_not_exists, model_name, sql_query_part)
            self.execute(sql_query)
        self.commit()

    # TODO: Pragma settings here


class Field:
    def __init__(self, *args, **kwargs):
        self.unique = kwargs.get('unique', False)
        self.null = kwargs.get('null', True)

    def get(self, value):
        return value

    def prepare(self, value):
        """Field preparation before been put to the SQL query"""
        return value

    def validate(self, value):
        """Field validation. Should be overridden.
           Returns True if field meets the type and False otherwise."""
        return True

    def __get_sqlite_def__(self):
        sql_part = self.name + " "
        sql_part += self.__sqlite_field_name__
        if not self.null:
            sql_part += " NOT NULL"
        if self.unique:
            sql_part += " UNIQUE"
        return sql_part


class TextField(Field):
    __sqlite_field_name__ = "TEXT"

    def prepare(self, value):
        value_escaped = value.replace("'","''")
        return "'%s'" % value_escaped

    def validate(self, value):
        return isinstance(value, str)


class IntegerField(Field):
    __sqlite_field_name__ = "INTEGER"


class RealField(Field):
    __sqlite_field_name__ = "REAL"


class PrimaryKey(Field):
    __sqlite_field_name__ = "INTEGER PRIMARY KEY"

    def __init__(self, autoincrement=False):
        super().__init__(self)
        self.autoincrement = autoincrement

    def __get_sqlite_def__(self):
        x = super().__get_sqlite_def__()
        if self.autoincrement:
            x += " AUTOINCREMENT"
        return x


class ForeignKey(Field):
    __sqlite_field_name__ = "INTEGER"

    def get(self, value):
        bind_obj = self.bind_model.get(id=value)
        return bind_obj

    def prepare(self, bind_model):
        return '%d' % bind_model['id']

    def __init__(self, bind, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        self.bind_model = bind


class ModelClass(type):
    @classmethod
    def __prepare__(metacls, name, bases, **kwds):
        return OrderedDict()

    def __new__(cls, name, bases, namespace, **kwds):
        fields = OrderedDict()
        # save fields to OrderedDict model.fields
        x = list(namespace.items())
        for field_name, field_obj in x:
            if isinstance(field_obj, Field):
                del namespace[field_name]
                fields[field_name] = field_obj

        result = type.__new__(cls, name, bases, namespace)
        result.fields = fields

        # save database info - list of models and let Model know about its name
        if name != 'Model':
            result._Model__database = Database()
            result._Model__database.models[name] = result
            result._name = name

        # save field name to field object
        for f_name, f in result.fields.items():
            f.name = f_name
        return result


class Model(metaclass=ModelClass):

    @classmethod
    def get(cls, **kwargs):
        field_names = ", ".join(cls.fields)
        c = cls._Model__database.connection.cursor()
        where_list = ['%s="%s"' % (k, str(v)) for k,v in kwargs.items()]
        where_string = "WHERE " + " AND ".join(where_list) if where_list else ""
        sql = "SELECT %s FROM %s %s;" % (field_names, cls._name, where_string)
        logging.debug(sql)
        c.execute(sql)
        column_names = [x[0] for x in c.description]
        query_result = c.fetchone()
        if query_result:
            query_dict = {}
            for (key, value) in zip(column_names, query_result):
                field = cls.fields[key]
                query_dict[key] = field.get(value)
            obj = cls(**query_dict)
            obj._new = False
            return obj

    @classmethod
    def get_all(cls, **kwargs):
        field_names = ", ".join(cls.fields)
        c = cls._Model__database.connection.cursor()
        where_list = ['%s="%s"' % (k, str(v)) for k,v in kwargs.items()]
        where_string = "WHERE " + " AND ".join(where_list) if where_list else ""
        sql = "SELECT %s FROM %s %s;" % (field_names, cls._name, where_string)
        logging.debug(sql)
        c.execute(sql)
        column_names = [x[0] for x in c.description]
        query_results = c.fetchall()
        if query_results:
            obj_list=[]
            for query_result in query_results:
                query_dict = {key: value for (key, value) in zip(column_names, query_result)}
                obj = cls(**query_dict)
                obj._new = False
                obj_list.append(obj)
            return obj_list


    def presave(self):
        field_names = []
        field_values = []
        for field_name, field_value in self.field_dict.items():
            field_obj = self.fields[field_name]
            field_values.append(field_obj.prepare(field_value))
            field_names.append(field_name)
        fields_sets = ", ".join(["%s=%s" % (f_n, f_v) for f_v, f_n in zip(field_values, field_names)])
        try:
            if self._new:
                sql = "INSERT INTO %s (%s) VALUES (%s);" % (self._name, ", ".join(field_names), ", ".join(field_values))
            else:
                sql = "UPDATE %s SET %s WHERE id=%d;" % (self._name, fields_sets, self['id'])
            self.__database.execute(sql)
        except sqlite3.IntegrityError as e:
            logging.error(str(e))

    def commit(self):
        self.__database.commit()

    def save(self):
        self.presave()
        self.commit()

    def __init__(self, **kwargs):
        self.field_dict = {}
        for field_name, field_value in kwargs.items():
            if field_name in self.fields:
                if self.fields[field_name].validate(field_value):
                    self.field_dict[field_name] = kwargs[field_name]
        self._new = True

    def __getattr__(self, item):
        if item in self.fields:
            return self.field_dict.get(item)

    def __setattr__(self, item, value):
        if item in self.fields:
            self.field_dict[item] = value
        else:
            object.__setattr__(self, item, value)

    def __getitem__(self, item):
        return self.field_dict.get(item)

    def __setitem__(self, key, value):
        self.field_dict[key] = value

    def __repr__(self):
        return self.field_dict.__repr__()