class PyFlowError(Exception):
     pass
class DataSourceError(PyFlowError):
     pass
class ValidationError(PyFlowError): 
     pass
class TransformationError(PyFlowError): 
     pass
class LoadError(PyFlowError): 
     pass