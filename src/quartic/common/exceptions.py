import traceback

class RunnerException(Exception):
    pass

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
    def __init__(self, step_id, steps):
        super(NoMatchingStepsException, self).__init__("No matching steps")
        self.step_id = step_id
        self.steps = steps

class UserCodeExecutionException(RunnerException):
    def __init__(self, exception, tb):
        super(UserCodeExecutionException, self).__init__("Exception while executing user code")
        extracted_tb = traceback.extract_tb(tb)
        self._exception = exception
        self.formatted_exception = traceback.format_exc()
        self.traceback = traceback.format_tb(tb)
        self.file_name = extracted_tb[-1].filename
        self.line_number = extracted_tb[-1].lineno
        self.exception_type = type(exception).__name__
        self.exception_args = exception.args

    def exception(self):
        return self._exception

class QuarticException(Exception):
    pass
    