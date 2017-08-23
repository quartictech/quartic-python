class RunnerException(Exception):
    def __init__(self, message):
        super(RunnerException, self).__init__(message)

class ArgumentParserException(RunnerException):
    def __init__(self, parser, message):
        super(ArgumentParserException, self).__init__(message)
        self.message = message
        self.parser = parser

class MultipleMatchingStepsException(RunnerException):
    def __init__(self, step_id, steps):
        super(MultipleMatchingStepsException, self).__init__("Multiple matching steps")
        self.step_id = step_id
        self.steps = steps

class NoMatchingStepsException(RunnerException):
    def __init__(self, step_id):
        super(NoMatchingStepsException, self).__init__("No matching steps")
        self.step_id = step_id

class UserCodeExecutionException(RunnerException):
    def __init__(self, exception, formatted_exception, tb, file_name,
                 line_number, exception_type, exception_args):
        super(UserCodeExecutionException, self).__init__("Exception while executing user code")
        self._exception = exception
        self.formatted_exception = formatted_exception
        self.traceback = tb
        self.file_name = file_name
        self.line_number = line_number
        self.exception_type = exception_type
        self.exception_args = exception_args

class ModuleNotFoundException(RunnerException):
    def __init__(self, module):
        super(ModuleNotFoundException, self).__init__("Module not found", 5)
        self.module = module

class QuarticException(Exception):
    pass
