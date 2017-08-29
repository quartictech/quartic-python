from .common.quartic import Quartic

try:
    from IPython.core.magic import Magics, magics_class, line_magic

    @magics_class
    class QuarticMagics(Magics):
        @line_magic
        def quartic_connect(self, line):
            self.shell.push({"quartic": Quartic("http://{service}.platform:{port}/api/", shell=self.shell)})
            print("Connected to Quartic services.")

        @line_magic
        def quartic_connect_local(self, line):
            self.shell.push({"quartic": Quartic()})
            print("Connected to Quartic services at localhost.")
except ImportError:
    print("Exception while importing iPython; disabling magics.")

def load_ipython_extension(shell):
    shell.register_magics(QuarticMagics)
    shell.push("MetaData")

def unload_ipython_extension(shell):
    pass
