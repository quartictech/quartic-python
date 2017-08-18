class RunnerException(Exception):
    def __init__(self, message, exit_code):
        super(RunnerException, self).__init__(message)
        self.exit_code = exit_code

class ArgumentParserException(RunnerException):
    def __init__(self, parser, message):
        super(ArgumentParserException, self).__init__(message, 1)
        self.message = message
        self.parser = parser

class MultipleMatchingStepsException(RunnerException):
    def __init__(self, step_id, steps):
        super(MultipleMatchingStepsException, self).__init__("Multiple matching steps", 2)
        self.step_id = step_id
        self.steps = steps

class NoMatchingStepsException(RunnerException):
    def __init__(self, step_id):
        super(NoMatchingStepsException, self).__init__("No matching steps", 3)
        self.step_id = step_id

class UserCodeExecutionException(RunnerException):
    def __init__(self, exception, tb, file_name,
                 line_number, exception_type, exception_args):
        super(UserCodeExecutionException, self).__init__("Exception while executing user code", 4)
        self.exception = exception
        self.traceback = tb
        self.file_name = file_name
        self.line_number = line_number
        self.exception_type = exception_type
        self.exception_args = exception_args

class ModuleNotFoundException(RunnerException):
    def __init__(self, module):
        super(ModuleNotFoundException, self).__init__("Module not found", 5)
        self.module = module