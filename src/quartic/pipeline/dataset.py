from quartic.utils import raise_if_invalid_coord, QuarticException

class Dataset:
    def __init__(self, dataset_id, namespace=None):
        if isinstance(dataset_id, Dataset):
            self.namespace = dataset_id.namespace
            self.dataset_id = dataset_id.dataset_id
        else:
            self.namespace = None if namespace is None else str(namespace)
            self.dataset_id = str(dataset_id)
            raise_if_invalid_coord(self.namespace, self.dataset_id)

    def __repr__(self):
        if self.namespace:
            return "{}::{}".format(self.namespace, self.dataset_id)
        else:
            return "::{}".format(self.dataset_id)

    def __hash__(self):
        return hash((self.namespace, self.dataset_id))

    def __eq__(self, other):
        return (isinstance(other, Dataset) and
                self.namespace == other.namespace and self.dataset_id == other.dataset_id)

    def with_namespace(self, namespace):
        return Dataset(self.dataset_id, namespace)

    def fully_qualified(self, default_namespace):
        if self.namespace:
            return self
        else:
            return Dataset(self.dataset_id, default_namespace)

    def resolve(self, quartic):
        if not self.namespace:
            raise QuarticException("Dataset not fully qualified: {}".format(self))
        return quartic(self.namespace).dataset(self.dataset_id)

    def to_json(self):
        return {
            "namespace": self.namespace, 
            "dataset_id": self.dataset_id
        }

    @staticmethod
    def parse(s):
        bits = s.split("::")
        if not bits[0]:
            return Dataset(bits[1])
        return Dataset(bits[1], bits[0])

class Writer:
    def __init__(self, name, description):
        self._name = name
        self._description = description
        self._exec = lambda f: None

    def parquet(self, df):
        self._exec = lambda f: f.parquet(df)
        return self

    def json(self, df):
        self._exec = lambda f: f.json(df)
        return self

    def apply(self, dataset):
        with dataset.writer(self._name, self._description) as f:
            self._exec(f)

def writer(name, description=None):
    return Writer(name, description)

