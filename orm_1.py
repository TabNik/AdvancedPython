#! /usr/bin/python3.6

import gc
import inspect
import pymysql 

#+ -- добавления/изменения

# исключения
class ValidationError(Exception):
    """..."""
    def __init__(self, message='validation error.'):
        print(message)

# базовый класс всех полей
class Field:
    # конструктор поля
    # тип поля, "обязательность", значение по умолчанию
    def __init__(self, field_name='', field_type=None, required=True, default=None):
        self.field_name = '_' + field_name
        self.field_type = field_type
        self.required = required
        self.default = default
        self.sql_name = ''

    def __get__(self, instance, owner=None):
        #
        return getattr(instance, self.field_name, self.default)
    def __set__(self, instance, value):
        instance.__dict__[self.field_name] = value
    def __delete__(self, instance):
        #
        del instance.__dict__[self.field_name]

# наследники-классы (разные типы полей)
#####################################################
# int, dec
# string (varchar)
# 
# date, time ?

class IntField(Field):
    # INT
    def __init__(self, field_name='', min_value=None, max_value=None, **kwargs):
        self.min_value = -2**31
        self.max_value =  2**31 - 1
        super().__init__(field_name, int, **kwargs)
    #+
    @property
    def get_sql_name(self):
        return 'INT(10)'
    def __get__(self, instance, owner=None):
        return super().__get__(instance)
    def __set__(self, instance, value):
        if not (self.min_value <= value <= self.max_value):
            raise ValidationError(message='the |value| is too big.')
        super().__set__(instance, value)
    def __delete__(self, instance):
        super().__delete__(instance)

class StrField(Field):
    # CHAR
    def __init__(self, field_name='', min_len=None, max_len=None, **kwargs):
        self.min_len = 1
        self.max_len = 8000
        super().__init__(field_name, str, **kwargs)
    #+
    @property
    def get_sql_name(self):
        return 'CHAR(255)'
    def __get__(self, instance, owner=None):
        return super().__get__(instance)
    def __set__(self, instance, value):
        if not (self.min_len <= len(value) <= self.max_len):
            raise ValidationError(message='the length of value is too big.')
        super().__set__(instance, value)
    def __delete__(self, instance):
        super().__delete__(instance)

class FloatField(Field):
    # FLOAT
    def __init__(self, field_name='', min_value=None, max_value=None, **kwargs):
        self.min_value = -1.79e+38
        self.max_value =  1.79e+38
        super().__init__(field_name, float, **kwargs)
    #+
    @property
    def get_sql_name(self):
        return 'FLOAT(53,8)'
    def __get__(self, instance, owner=None):
        return super().__get__(instance)
    def __set__(self, instance, value):
        if not (self.min_value <= value <= self.max_value):
            raise ValidationError(message='the |value| is too big.')
        super().__set__(instance, value)
    def __delete__(self, instance):
        super().__delete__(instance)

#####################################################

# метакласс, формирует классы (модели)
class ModelMeta(type):
    def __new__(mcs, name, bases, namespace):
        super_new = super().__new__
        # "ensure initialization is only performed for subclasses of Model"
        # (c) Django
        parents = [i for i in bases if isinstance(i, ModelMeta)]
        if not parents:
            return super_new(mcs, name, bases, namespace)
        #
        # создание класса
        new_namespace = {} # атрибуты будущего класса
        module = namespace.pop('__module__')
        new_namespace['__module__'] = module
        
        new_class = super_new(mcs, name, bases, namespace)
        
        ### далее -- формирование нового класса new_class
        fields  = {name: value for name, value in namespace.items() if isinstance(value, Field)}
        methods = {name: value for name, value in namespace.items() if inspect.isfunction(value)}

        fields_names = set(fields.keys())

        meta = namespace.pop('Meta', None)
        if meta is not None:
            if not hasattr(meta, 'table_name'):
                setattr(meta, 'table_name', name)
            if not hasattr(meta, 'fields'):
                setattr(meta, 'fields', fields)
        else:
            meta = type('Meta', tuple(), {'table_name': name, 'fields': fields_names})

        # наследование полей по MRO
        for base_class in new_class.mro():
            for field_name, field_value in base_class.__dict__.items():
                # если в базовом классе обнаружено поле, оно записывается в потомка
                #  и переходит к следующему базовому; иначе записывает метод
                if isinstance(field_value, Field):
                    fields.update({field_name: field_value})
                    continue
                if inspect.isfunction(field_value):
                    methods.update({field_name: field_value})
        
        #

        meta.fields.update(set(fields.keys()))
        fields.update(mcs._set_manager(namespace))
        setattr(new_class, 'Meta', meta)
        for field_name, field_value in fields.items():
            setattr(new_class, field_name, field_value)

        return new_class
        ###

    #

    @classmethod
    def _set_manager(cls, namespace=None):
        if namespace is None:
            return None
        managers = {name: value for name, value in namespace.items() if isinstance(value, Manage)}
        if len(managers) == 0:
            managers.update({'objects': Manage()})
        if len(managers) > 1:
            raise AttributeError('Table have to has only one Manage()')
        return managers

##
class MetaManage(type):
    def __new__(mcs, name, bases, namespace):
        return super().__new__(mcs, name, bases, namespace)
 
class Manage(metaclass=MetaManage):
    def __get__(self, instance, owner=None):
        return QuerySet(owner=owner)
    
    def __delete__(self, instance):
        super().__delete__(instance)
##
#+
class QuerySet:
    def __new__(cls, **kwargs):
        return super().__new__(cls)
    
    def __init__(self, owner=None, query_set=None):
        if owner is None:
            raise AttributeError('Owner is None')
        self.cls = owner
        if query_set is None:
            self.query_set = []
        else:
            self.query_set = query_set

    def __len__(self):
        return len(self.query_set)
    def __getitem__(self, index):
        return self.query_set[index]
    
    def create(self, **kwargs):
        return self.cls(**kwargs)
        
    #+ метод извлечения всех данных
    def all(self):
        #
        connection = connect()
        cursor = connection.cursor()
        #
        #+
        table_name = self.cls.Meta.table_name
        sql_query = 'SELECT * FROM ' + table_name + ' ORDER BY id;'

        cursor.execute(
            sql_query
        )
        self.query_set += [self.cls(**line) for line in cursor.fetchall()]

        connection.close()
        gc.collect()
        return self
    
    #+ метод извлечения данных по ключу
    def get(self, key=None, **kwargs):
        to_get_from = self.all()
        return to_get_from[key - 1]

    #+
    def delete(self, key=None, **kwargs):
        #
        connection = connect()
        cursor = connection.cursor()
        #
        table_name = self.cls.Meta.table_name

        prepared_statement_delete = 'DELETE FROM ' + table_name + ' WHERE id = %s'
        cursor.execute(
            prepared_statement_delete,
            key
        )

        connection.commit()
        connection.close()
        gc.collect()


# подключение к базе
def connect():
    return pymysql.connect(host='localhost', 
                           user='root',
                           password='3235',
                           db='my_db',
                           cursorclass=pymysql.cursors.DictCursor)

# модели (создают отношения)
class Model(metaclass=ModelMeta):
    class Meta:
        table_name = 'Model'
        fields = {}

    #
    objects = Manage()

    def __init__(self, **kwargs):
        for field_name, field_value in kwargs.items():
            if field_name in self.__class__.Meta.fields:
                setattr(self, field_name, field_value)
    
    def _get_name_value_id(self):
        for name, value in self.__class__.__dict__.items():
            if (isinstance(value, Field) and name == 'id'):
                return name, getattr(self, name)

    def _quotes_if_str(self, value):
        if isinstance(value, str):
            return "'" + value + "'"
        return value

    #+
    def _table_creation(self):
        connection = connect()
        cursor = connection.cursor()

        table_name = self.__class__.Meta.table_name

        prepared_statement_create = 'CREATE TABLE IF NOT EXISTS ' + table_name + ' ('
        for field_name in self.__class__.Meta.fields:
            prepared_statement_create   += field_name + ' ' + self.__class__.__dict__[field_name].get_sql_name + ','

        prepared_statement_create   = prepared_statement_create[:-1] + ');'

        cursor.execute(
            prepared_statement_create
            )
        
        connection.commit()
        connection.close() 
        gc.collect()
        

    #+
    def update(self):
        #
        connection = connect()
        cursor = connection.cursor()
        #+
        table_name = self.__class__.Meta.table_name

        prepared_statement_update = 'UPDATE {0} SET {1} WHERE {2} = {3};'
        name, value = self._get_name_value_id()

        #+
        list_of_upd_values = [
            str(f_name) + '=' + str(self._quotes_if_str(getattr(self, f_name)))
            for f_name, f_value in self.__class__.__dict__.items()
            if isinstance(f_value, Field)
        ]
        list_of_upd_values = ','.join(list_of_upd_values)
        prepared_statement_update = prepared_statement_update.format(
            table_name,
            list_of_upd_values,
            name,
            value
        )
        
        cursor.execute(
            prepared_statement_update
        )

        connection.commit()
        connection.close()
        gc.collect()

    def save(self):
        #+
        self._table_creation()
        ### подключение к бд
        connection = connect()
        cursor = connection.cursor()
        ###

        #+
        table_name = self.__class__.Meta.table_name

        prepared_statement_insert_1 = 'INSERT INTO ' + table_name + ' ('
        prepared_statement_insert_2 = ') VALUES ('
        list_of_fvalues = []
        for field_name in self.__class__.Meta.fields:
            #
            list_of_fvalues.append(getattr(self, field_name))
            prepared_statement_insert_1 += field_name + ','
            prepared_statement_insert_2 += '%s,'
            #
        prepared_statement_insert = prepared_statement_insert_1[:-1] + prepared_statement_insert_2[:-1] + ');'

        #+ вместо '...{}'.format() -- агрументы курсора

        cursor.execute(
            prepared_statement_insert,
            list_of_fvalues
            )
        
        connection.commit()
        connection.close() 
        gc.collect()
        


#### непосредственно использование
# (a view, rapped in the class)
# наследуется от модели, сужаясь на конкретную таблицу
class User(Model):
    
    id        = IntField('id',required=True, default=0)
    Telephone = IntField('Telephone',required=True, default=100)
    Name      = StrField('Name',required=True, default='some_name')
    Sex       = StrField('Sex',required=True, default=True)
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def update_telephone(self, value):
        self.Telephone = value
        
class Admin(User):
    Level = IntField(required=True, default = -1)

## проверка добавления
a = Admin.objects.create(id=1, Telephone=14513451, Name='kirill', Sex='Male')
b = Admin.objects.create(id=2, Telephone=492019, Name='nikita', Sex='Male', Level=100)
c = Admin.objects.create(id=3, Telephone=30434, Name='sergey', Sex='Male', Level=99)
b.save()
a.save()
c.save()

## проверка чтения
# d1 = Admin.objects.get(key=1)
# d3 = Admin.objects.get(key=3)
# d2 = Admin.objects.get(key=2)
# d1.save()
# d2.save()
# d3.save()

# проверка обновления
# b.Name = 'New Name'
# b.update()

# проверка удаления
# Admin.objects.delete(key=1)
# Admin.objects.delete(key=3)


