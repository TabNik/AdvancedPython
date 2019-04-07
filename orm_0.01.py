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

    def __get__(self, instance, owner=None):
        #
        return getattr(instance, self.field_name, self.default)
    def __set__(self, instance, value):
        instance.__dict__[self.field_name] = value
    def __delete__(self, instance):
        #
        del instance.__dict__[self.field_name]
    
    # возвращает значение приведенного типа
    def validate(self, value):
        # if not isinstance(value, self.field_type):
        #     raise ValidationError(message='value & type mismatch.')
        if value is None and not self.required:
            return None
        if value is None and self.required:
            raise ValidationError(message='field is required.')
        return self.field_type(value)

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
        return QuerySet(owner)
    
    def __delete__(self, instance):
        super().__delete__(instance)
##
# ...
class QuerySet:
    _query_set = []
    
    def __init__(self, owner=None):
        if owner is None:
            raise AttributeError('Owner is None')
        self.cls = owner
    
    def create(self, **kwargs):
        return self.cls(**kwargs)
        
    def all(self):
        pass
    
    def get(self, **kwargs):
        pass

# модели (создают отношения)
class Model(metaclass=ModelMeta):
    class Meta:
        table_name = 'Model'
        fields = {} # id

    #
    objects = Manage()

    def __init__(self, **kwargs):
        for field_name, field_value in kwargs.items():
            if field_name in self.__class__.__dict__['Meta'].__dict__['fields']:
                setattr(self, field_name, field_value)
    
    #

    def save(self):
        ### подключение к бд
        connection = pymysql.connect(host='localhost',
                             user='root',
                             password='3235',
                             db='my_db',
                             cursorclass=pymysql.cursors.DictCursor)
        cursor = connection.cursor()
        ###

        types = {IntField: 'INT(10)', FloatField: 'FLOAT(53,8)', StrField: 'CHAR(255)'}
        table_name = self.__class__.__dict__['Meta'].__dict__['table_name']

        prepared_statement_create = 'CREATE TABLE IF NOT EXISTS ' + table_name + ' ('
        prepared_statement_insert_1 = 'INSERT INTO ' + table_name + ' ('
        prepared_statement_insert_2 = ') VALUES ('
        list_of_fvalues = []
        for field_name in self.__class__.__dict__['Meta'].__dict__['fields']:
            #
            list_of_fvalues.append(getattr(self, field_name))
            prepared_statement_create   += field_name + ' ' + types[type(self.__class__.__dict__[field_name])] + ','
            prepared_statement_insert_1 += field_name + ','
            prepared_statement_insert_2 += '%s,'
            #
        prepared_statement_create   = prepared_statement_create[:-1] + ');'
        prepared_statement_insert = prepared_statement_insert_1[:-1] + prepared_statement_insert_2[:-1] + ');'

        #+ вместо '...{}'.format() -- агрументы курсора
        cursor.execute(
            prepared_statement_create
            )
        
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
    
    id_       = IntField('id_',required=True, default=0)
    Telephone = IntField('Telephone',required=True, default=100)
    Name      = StrField('Name',required=True, default='some_name')
    Sex       = StrField('Sex',required=True, default=True)
    
    def __init__(self, **kwarg):
        super().__init__(**kwarg)
    
    def update_telephone(self, value):
        self.Telephone = value

# (an object instance is tied to a single row in the table)
class Admin(User):
    Level = IntField(required=True, default = -1)

a = Admin.objects.create(id_=1, Telephone=14513451, Name='kirill', Sex='Male')
b = Admin.objects.create(id_=2, Telephone=492019, Name='nikita', Sex='Male', Level=100)
a.save()
b.save()

c = Admin.objects.create(id_=3, Telephone=30434, Name='sergey', Sex='Male', Level=99)
c.save()



