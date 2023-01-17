int_min = -2147483648
int_max = 2147483647
float_min = -1.7976931348623157e+308
float_max = 1.7976931348623157e+308
code_text = ''
code_python = 'script.py'

port_type_any = '通用'
port_type_csv = 'CSV'

port_name_lazy_run = 'm_lazy_run'


class BaseType:
    def __init__(self, type_name, desc=None, specific_type_name=None):
        self.type_name = type_name
        self.desc = desc
        self.specific_type_name = specific_type_name

    def to_json(self):
        d = self.__dict__.copy()
        d['type_code'] = self.__class__.__name__
        return d


class BaseNumber(BaseType):
    def __init__(self, type_name, desc, min, max):
        super().__init__(type_name, desc)
        self.min = min
        self.max = max


class int(BaseNumber):
    def __init__(self, desc=None, min=int_min, max=int_max):
        super().__init__('Int', desc, min, max)


class float(BaseNumber):
    def __init__(self, desc=None, min=float_min, max=float_max):
        super().__init__('Float', desc, min, max)


class choice(BaseType):
    def __init__(self, desc=None, values=[], multi=False):
        super().__init__('Enumerated', desc)
        self.values = values
        self.multi = multi


class code(BaseType):
    def __init__(self, desc=None, language='', default=None, specific_type_name=None, auto_complete_type=None):
        super().__init__('Script', desc, specific_type_name=specific_type_name)
        self.language = language
        self.default = default
        self.auto_complete_type = auto_complete_type


class str(BaseType):
    def __init__(self, desc=None, specific_type_name=None, can_set_liverun_param=None):
        super().__init__('String', desc, specific_type_name=specific_type_name)
        self.can_set_liverun_param = can_set_liverun_param


class bool(BaseType):
    def __init__(self, desc=None, specific_type_name=None):
        super().__init__('Boolean', desc, specific_type_name=specific_type_name)


class port(BaseType):
    def __init__(self, desc=None, name=None, optional=False, type=port_type_any, specific_type_name=None):
        super().__init__(None, desc, specific_type_name=specific_type_name)
        self.name = name
        self.optional = optional
        self.type = type


class doc(BaseType):
    def __init__(self, desc=None, name=None, specific_type_name=None):
        # for doc only
        super().__init__('doc', desc, specific_type_name=specific_type_name)
        self.name = name
